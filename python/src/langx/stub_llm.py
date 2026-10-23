"""Deterministic stub LLM that replays recorded responses.

Responsibilities (plan section 11):
    * look up responses in a JSON fixture keyed by SHA-256 of the canonical
      prompt
    * treat a fixture miss as an error, never fall back to a real model

Not implemented yet.
"""
