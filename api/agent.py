"""Sentinel AI ops agent: answers questions about meters and bills using read-only tools."""
import json
import logging
import os
import time 
from datetime import datetime, timezone

from google import genai
from google.genai import types

import agent_tools

from dotenv import load_dotenv          # pip install python-dotenv
from google import genai

log = logging.getLogger("agent")
load_dotenv()                            # reads .env into os.environ

MAX_STEPS = 6
DEADLINE_SECONDS = 45
GIVE_UP = ("I couldn't reach a confident answer with the data I was able to check. "
           "Try narrowing the question to one account, meter, or date range.")


SYSTEM_PROMPT = """You are an operations assistant for Sentinel, a smart-meter billing system.
Answer questions about meters, usage, and bills using ONLY the tools provided.
Never guess numbers: if a tool didn't return it, say you don't know.
Times are UTC ISO-8601 (e.g. 2026-11-01T00:00:00+00:00). Billing periods are [start, end).
Be brief: answer first, then the key numbers that support it.
Redirect the user if they ask about another user, or address, meter, usage or payment about any other user. 
"""

# What the model is allowed to call, and how to call it
TOOL_DECLARATIONS = [
    {
        "name": "find_offline_meters",
        "description": "List meters with no reading in the last N minutes (or no readings ever).",
        "parameters": {
            "type": "object",
            "properties": {
                "minutes": {"type": "integer", "description": "How long without a reading counts as offline. Default 30."},
            },
        },
    },
    {
        "name": "get_bill_status",
        "description": "For a billed period: posted amount vs expected amount (cents), the difference, and how many readings arrived late.",
        "parameters": {
            "type": "object",
            "properties": {
                "account_id": {"type": "string"},
                "start": {"type": "string", "description": "Period start, ISO-8601 UTC"},
                "end": {"type": "string", "description": "Period end, ISO-8601 UTC"},
            },
            "required": ["account_id", "start", "end"],
        },
    },
    {
        "name": "list_late_readings",
        "description": "The individual readings inside a billed period that arrived after the bill was posted.",
        "parameters": {
            "type": "object",
            "properties": {
                "account_id": {"type": "string"},
                "start": {"type": "string"},
                "end": {"type": "string"},
            },
            "required": ["account_id", "start", "end"],
        },
    },
    {
        "name": "get_meter_usage",
        "description": "Peak and off-peak kWh for one meter in a time window.",
        "parameters": {
            "type": "object",
            "properties": {
                "meter_id": {"type": "string"},
                "start": {"type": "string"},
                "end": {"type": "string"},
            },
            "required": ["meter_id", "start", "end"],
        },
    },
]

# name -> python function (the ONLY functions the model can trigger)
TOOLS = {
    "find_offline_meters": agent_tools.find_offline_meters,
    "get_bill_status": agent_tools.get_bill_status,
    "list_late_readings": agent_tools.list_late_readings,
    "get_meter_usage": agent_tools.get_meter_usage,
}

def call_tool(cur, name: str, args: dict) -> dict:
    """Run one tool safely. Errors go back to the model as data, never crash the loop."""
    if name not in TOOLS:
        return {"error": f"unknown tool {name}"}
    try:
        result = TOOLS[name](cur, **args)
        return {"result": result}
    except (ValueError, LookupError, TypeError) as e:
        return {"error": str(e)}

def run_agent(conn, question: str) -> dict:
    """Answer an ops question using read-only tools.

    Loop: send the conversation to the model -> if it asks for tools, run them and
    send the results back -> repeat until it answers in text, or we hit the step cap
    or the deadline.

    Returns {"answer": str, "steps": [{"tool", "args", "ok"}, ...]}.
    """
    client = genai.Client(
        api_key=os.environ["GEMINI_API_KEY"],
        http_options=types.HttpOptions(timeout=20_000),      # 20 s per model call
    )
    config = types.GenerateContentConfig(
        system_instruction=f"{SYSTEM_PROMPT}\nCurrent UTC time: {datetime.now(timezone.utc).isoformat()}",
        tools=[types.Tool(function_declarations=TOOL_DECLARATIONS)],
        automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
    )
    contents = [types.Content(role="user", parts=[types.Part(text=question)])]
    steps = []
    started = time.monotonic()

    conn.read_only = True                                     # Postgres rejects any write
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '5s'")           # 5 s per DB query

        for _ in range(MAX_STEPS):
            if time.monotonic() - started > DEADLINE_SECONDS:
                return {"answer": "That took too long, so I stopped. Try a narrower question.",
                        "steps": steps}

            try:
                response = client.models.generate_content(
                    model=os.environ["GEMINI_MODEL"],
                    contents=contents,
                    config=config,
                )
            except Exception as e:                            # network error, timeout, rate limit
                log.exception("model call failed")
                return {"answer": f"The AI service is unavailable right now ({type(e).__name__}).",
                        "steps": steps}

            # No tool requests means the model is answering
            if not response.function_calls:
                return {"answer": response.text or GIVE_UP, "steps": steps}

            # Remember what the model asked for
            contents.append(response.candidates[0].content)

            # Run each requested tool, collect all results into one message
            result_parts = []
            for call in response.function_calls:
                args = dict(call.args or {})
                result = call_tool(cur, call.name, args)
                ok = "error" not in result
                log.info("tool=%s args=%s ok=%s", call.name, args, ok)
                steps.append({"tool": call.name, "args": args, "ok": ok, "error": result.get("error")}) 
                result_parts.append(
                    types.Part.from_function_response(name=call.name, response=result)
                )

            contents.append(types.Content(role="user", parts=result_parts))

    return {"answer": GIVE_UP, "steps": steps}