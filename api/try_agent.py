import logging

import agent
from main import db

logging.basicConfig(level=logging.INFO)   # shows each tool call as it happens

QUESTIONS = [
    "Which meters are offline?",
    "Why is acct-1's bill for 2026-11-01 to 2026-11-02 mismatched?",
    "How much peak vs off-peak energy did meter-1 use on 2026-11-01?",
]

for q in QUESTIONS:
    with db() as conn:
        out = agent.run_agent(conn, q)
    print("\nQ:", q)
    print("A:", out["answer"])
    print("steps:", out["steps"])