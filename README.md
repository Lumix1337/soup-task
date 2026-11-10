# Soup · Mini Experiment Tracker

Take-home task implementation for **Soup Full-Stack Engineer Internship**.

Answers two core questions with automated proofs:
1. **Consistency:** *Does what the user sees always match what is stored?*
2. **Isolation:** *Do they see only what's theirs?*

## Quick Start

```bash
docker compose up --build
```

- **Web UI:** `http://localhost:8000`
- **Health Check:** `http://localhost:8000/health`

## Documentation & Deliverables

- [`docs/DESIGN.md`](docs/DESIGN.md) — 1-page architecture design committed before code.
- [`NOTE.md`](NOTE.md) — <=2-page technical note covering consistency, isolation, silent failures, 50-run load benchmark verdict, answers to discussion questions, surprises/concerns, and AI tool verification log.
- [`docs/AI_LOG.md`](docs/AI_LOG.md) — Detailed log of AI-assisted suggestions accepted, rejected, and rewritten.
- `results/` — Unedited raw outputs from consistency, isolation, silent failure tests, and the 50-run benchmark.
