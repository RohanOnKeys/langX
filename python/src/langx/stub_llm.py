"""Deterministic stub LLM that replays recorded responses (plan section 11).

Responses live in a JSON fixture keyed by the SHA-256 of the canonical
prompt. A miss is an error; there is no fallback to a real model. Fixtures
are produced by record mode, which answers each prompt with a deterministic
generator and remembers the answer.
"""

import hashlib
import json
import pathlib


class StubMiss(KeyError):
    """The prompt has no recorded response."""


def canonical(obj):
    """Canonical JSON: sorted keys, no whitespace."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def prompt_key(prompt):
    """SHA-256 of the prompt text."""
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()


class StubLLM:
    """Replay responses by prompt hash, or record them from a generator."""

    def __init__(self, responses=None, generator=None):
        self.responses = dict(responses or {})
        self.generator = generator

    @classmethod
    def from_file(cls, path):
        """Load a replay-only stub from a fixture file."""
        return cls(json.loads(pathlib.Path(path).read_text()))

    def complete(self, prompt):
        """Return the recorded response for ``prompt``."""
        key = prompt_key(prompt)
        if key in self.responses:
            return self.responses[key]
        if self.generator is None:
            raise StubMiss(f"no recorded response for prompt {key[:12]}")
        self.responses[key] = self.generator(prompt)
        return self.responses[key]

    def save(self, path):
        """Write the recorded responses as a fixture file."""
        pathlib.Path(path).write_text(
            json.dumps(self.responses, indent=1, sort_keys=True) + "\n")
