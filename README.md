# Resilient Caching Library

The caching patterns you learn about after an outage.

## Why this project exists

A hot key expires, a thousand requests miss at once, and the database falls over. Most teams write a naive Get-or-Set and carry that risk without noticing it until a production incident exposes the flaw.

## What the system does

A Go library with single-flight stampede protection, probabilistic early expiry, negative caching, and a circuit breaker for a struggling backing store. Each pattern ships with a benchmark showing the failure it prevents, and there is a reproducible stampede scenario showing the naive cache melt compared with the protected version.

## Scope

- Single-flight/lock-based stampede protection
- Probabilistic early recompute
- Negative and tiered caching
- Circuit breaker on backing store
- Eviction policy comparison harness
- Reproducible stampede failure demo
- Instrumentation for hit rate and latency

## Deliverables

- GitHub repo (importable module)
- Benchmark charts
- Reproducible stampede demo
- Pattern selection guide
- Grafana dashboard

## Success criteria

A graph showing a naive cache sending 4,000 QPS at the database on one key expiry, and the same test flat with single-flight in place. One make target reproduces both.

## Who this matters to

- Backend and platform engineering
- high-traffic services
- SRE-adjacent roles

## Open the project

```bash
cd /Users/mbiskho/Documents/code/portfolio/software-engineering
python3 -m http.server 8000
```

Then open: http://localhost:8000/resilient-caching-library/index.html

## Project assets

- [index.html](./index.html)
- [reports/project-flow.svg](./reports/project-flow.svg)
- [reports/project-overview.svg](./reports/project-overview.svg)

This project is one of the portfolio demos in the software engineering section and is intended to be used as a static, local-first systems artifact rather than as a production deployment.
