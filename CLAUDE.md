# CLAUDE.md

Ember is a **learning project**: SLO-aware, scale-to-zero LLM inference on Kubernetes
(see README.md for architecture and roadmap). The author must be able to explain every
line they commit. Optimize for understanding, not speed.

## How to work in this repo

- **Explain before code.** Before writing any code, explain in plain words:
  1. what the piece does,
  2. why it exists,
  3. one alternative you considered and why you rejected it.
  Keep the explanation under 10 lines.
- **One concept per step.** Do one thing, then stop. Don't bundle unrelated changes.
- **Don't create files that weren't asked for.** No speculative scaffolding, helpers,
  tests, configs, or docs unless requested.
- **Don't fix bugs you weren't asked to fix.** Some bugs are intentional so they can be
  measured (e.g. at-most-once delivery before M2). If you notice one, you may mention it
  in one line; do not change it.
- **docs/design-doc.md is the author's own reasoning.** Never write reasoning, decisions,
  or rationale into it. Only critique what the author has written, when asked.

## Stack

- Python 3.12 (venv in `.venv`; create with `py -3.12 -m venv .venv` — the default
  `python` on this machine is a different version)
- FastAPI (async)
- redis-py, asyncio API (`redis.asyncio`)
- httpx (async client)
