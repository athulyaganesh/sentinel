// Sentinel smart-meter simulator.
// Each simulated meter samples once per second, buffers readings, and publishes
// a JSON batch to MQTT topic  meters/<meter_id>/readings  with QoS 1.
//
// Build:  cmake -S simulator -B build && cmake --build build
// Run:    ./build/meter_sim --devices 100 --batch-seconds 5 --dup-rate 0.05

#include <mosquitto.h>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>
#include <thread>
#include <vector>

struct Reading {
    std::string msg_id;   // STABLE: generated once, reused on every resend
    std::string meter_id;
    long long   ts_ms;    // epoch millis when the reading was taken
    double      kwh;      // cumulative register value
};

struct Config {
    int    devices        = 100;
    int    batch_seconds  = 5;
    double dup_rate       = 0.05;   // fraction of batches resent (simulates lost ack)
    double malformed_rate = 0.01;   // fraction of junk messages (tests dead-letter path)
    const char* host      = "localhost";
    int    port           = 1883;
};

static long long now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

static std::string to_json(const std::vector<Reading>& batch) {
    std::string out = "[";
    char buf[256];
    for (size_t i = 0; i < batch.size(); ++i) {
        const auto& r = batch[i];
        std::snprintf(buf, sizeof(buf),
            "{\"msg_id\":\"%s\",\"meter_id\":\"%s\",\"ts_ms\":%lld,\"kwh\":%.4f}",
            r.msg_id.c_str(), r.meter_id.c_str(), r.ts_ms, r.kwh);
        out += buf;
        if (i + 1 < batch.size()) out += ",";
    }
    return out + "]";
}

int main(int argc, char** argv) {
    Config cfg;
    for (int i = 1; i + 1 < argc; i += 2) {
        std::string a = argv[i];
        if (a == "--devices") cfg.devices = std::atoi(argv[i + 1]);
        else if (a == "--batch-seconds") cfg.batch_seconds = std::atoi(argv[i + 1]);
        else if (a == "--dup-rate") cfg.dup_rate = std::atof(argv[i + 1]);
        else if (a == "--host") cfg.host = argv[i + 1];
    }

    mosquitto_lib_init();
    // Persistent session (clean_session=false) so the broker remembers us across reconnects.
    mosquitto* mosq = mosquitto_new("sentinel-simulator", /*clean_session=*/false, nullptr);
    if (!mosq || mosquitto_connect(mosq, cfg.host, cfg.port, 30) != MOSQ_ERR_SUCCESS) {
        std::fprintf(stderr, "failed to connect to MQTT at %s:%d\n", cfg.host, cfg.port);
        return 1;
    }
    mosquitto_loop_start(mosq);

    std::mt19937 rng(42);
    std::uniform_real_distribution<double> usage(0.0, 0.002);   // kWh per second
    std::uniform_real_distribution<double> coin(0.0, 1.0);

    std::vector<double>    register_kwh(cfg.devices, 0.0);
    std::vector<long long> seq(cfg.devices, 0);
    std::vector<std::vector<Reading>> buffer(cfg.devices);

    // TODO(you): stagger devices with a per-meter offset (hash of meter_id) so they don't all publish at once.
    // TODO(you): skip readings whose value didn't change (report-by-exception) + send a heartbeat instead.
    // TODO(you): inject late / out-of-order readings.
    for (int tick = 1;; ++tick) {
        for (int d = 0; d < cfg.devices; ++d) {
            std::string meter_id = "meter-" + std::to_string(d + 1);
            register_kwh[d] += usage(rng);
            Reading r{meter_id + "-" + std::to_string(++seq[d]), meter_id, now_ms(), register_kwh[d]};
            buffer[d].push_back(r);

            if (tick % cfg.batch_seconds == 0) {
                std::string topic   = "meters/" + meter_id + "/readings";
                std::string payload = to_json(buffer[d]);
                mosquitto_publish(mosq, nullptr, topic.c_str(), (int)payload.size(), payload.data(), /*qos=*/1, false);

                // Simulated lost ack: resend the SAME batch (same msg_ids). Ingestion must dedupe.
                if (coin(rng) < cfg.dup_rate)
                    mosquitto_publish(mosq, nullptr, topic.c_str(), (int)payload.size(), payload.data(), 1, false);

                // Malformed message to exercise the dead-letter path.
                if (coin(rng) < cfg.malformed_rate) {
                    const char* junk = "{not valid json";
                    mosquitto_publish(mosq, nullptr, topic.c_str(), 15, junk, 1, false);
                }
                buffer[d].clear();   // TODO(you): only clear after the broker acks (on_publish callback)
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}
