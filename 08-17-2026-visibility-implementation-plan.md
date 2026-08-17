# Implementation Plan — Usage and Resource-Metric Visibility

**Date:** 2026-08-17
**Status:** Plan. Companion to `08-17-2026-propsoal-for-usage-and-resource-metric-visibility.md`.
**Premise change from the proposal:** the proposal reasoned from *chart defaults* (`replicaCount: 1`,
one uvicorn process). This plan does not. It assumes production may run **N processes × M replicas**,
and every design decision below is chosen so that the answer does not change when N or M changes.

**Conventions:** **[FACT]** verified against the tree at `196f642`. **[GATE]** a blocking exit
criterion. **[DECISION]** a call this plan makes, with its reason.

---

## Table of Contents

1. [Three findings that revise the proposal](#1-three-findings-that-revise-the-proposal)
2. [The decision that makes topology irrelevant](#2-the-decision-that-makes-topology-irrelevant)
3. [Does this push us towards Redis?](#3-does-this-push-us-towards-redis)
4. [Gate 0 — ground truth before code](#4-gate-0--ground-truth-before-code)
5. [Phase 1 — multi-process integrity](#5-phase-1--multi-process-integrity)
6. [Phase 2 — the missing queue numbers](#6-phase-2--the-missing-queue-numbers)
7. [Phase 3 — schema and a database CI](#7-phase-3--schema-and-a-database-ci)
8. [Phase 4 — decouple `/metrics`, introduce the LiveState seam](#8-phase-4--decouple-metrics-introduce-the-livestate-seam)
9. [Phase 5 — the Redis backend](#9-phase-5--the-redis-backend)
10. [Phase 6 — upstream queue depth](#10-phase-6--upstream-queue-depth)
11. [Phase 7 — the views](#11-phase-7--the-views)
12. [Phase 8 — lifecycle: aggregates, compression, retention](#12-phase-8--lifecycle-aggregates-compression-retention)
13. [Phase 9 — admission control (separate decision)](#13-phase-9--admission-control-separate-decision)
13a. [Burst pathologies this plan did not originally address](#13a-burst-pathologies-this-plan-did-not-originally-address)
14. [Standing invariants, enforced by test](#14-standing-invariants-enforced-by-test)
15. [Corrections to the proposal](#15-corrections-to-the-proposal)
16. [Adversarial review — what changed](#16-adversarial-review--what-changed)

---

## 1. Three findings that revise the proposal

### 1.1 Yes — TimescaleDB is in use, and it is mandatory, not opportunistic

**[FACT]** `migrations/versions/i9j0k1l2m3n4_timescaledb_tracking.py:37` executes

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE
```

unconditionally on the PostgreSQL branch, then `create_hypertable('request_logs', 'time', ...)` and a
continuous aggregate `request_counts_hourly`. This is **not** a soft feature-detect. If the extension
is unavailable the migration raises, and because `entrypoint.sh:8` runs `flask db upgrade` before
`exec uvicorn`, **the container fails to start**. The compose example backs this with
`timescale/timescaledb:latest-pg17` (`docker-compose.yml.example:38`).

So: any PostgreSQL deployment of Lumen that is running at all **has Timescale**. Only the SQLite path
gets a plain table, and every `/api/usage/*` endpoint short-circuits on the dialect check there.

Your question was framed as *"if not TimescaleDB, but already Redis, then Redis is a good place."*
The tree says the opposite of both halves:

| | Proposal's assumption | Verified state |
|---|---|---|
| TimescaleDB | present | **present and load-bearing** — the app cannot boot on Postgres without it |
| Redis | "already a dependency" | **installed as a Python package, not deployed** — `chart/values.yaml` `redis.enabled: false`, and the only consumer is flask-limiter storage |

That inverts the storage argument rather than weakening it. The purpose-built time-series store with
retention, compression, continuous aggregates and foreign keys to `entities`/`model_configs` is
already provisioned, already migrated, already backed up with the primary database, and already the
thing `/usage` reads. Redis is a cache that the chart ships **single-replica, `strategy: Recreate`,
`persistence.enabled: false`** — i.e. a rollout or node drain deletes its contents.

**[DECISION]** No historical or per-user instrumentation goes to Redis. Ever. That is not a
close call once Timescale is known to be mandatory.

### 1.2 Your critique of the Redis argument is correct — but it does not lead to Redis

The proposal's §8 conclusion rests on one premise: *one pod, one process, therefore process-local
memory is exactly correct.* That premise is a reading of `chart/values.yaml:5` and `entrypoint.sh:9`
— **chart defaults, not verified production config.** You are right to distrust it. Two things
follow, and only the second is about Redis:

1. **Most of the proposal is topology-invariant and survives the critique untouched.** Columns on
   `request_logs`, Prometheus counters and histograms, the backend scraper, the retention plan — none
   of these care how many processes exist, because every process writes to the same database and
   Prometheus sums counters across scrape targets by construction.

2. **A specific, small set of numbers is topology-sensitive**, and for those the proposal's
   recommendation (process-local dict) is wrong under N>1. That set is enumerated in §2.

The mistake to avoid is generalising from (2) to "so use Redis for the instrumentation." The
topology-sensitive set is three numbers, and only one of them genuinely needs Redis.

### 1.3 Scaling beyond one process is not currently safe — for reasons unrelated to metrics

**[FACT]** Nothing in the tree runs more than one process today: `entrypoint.sh:9` is
`exec uvicorn asgi:app --host 0.0.0.0 --port 5001 $@` with no `--workers`. The code *anticipates*
multi-process — `db_pool.detect_workers()` reads `WEB_CONCURRENCY` and parses `--workers` off the
parent cmdline, `detect_replicas()` reads `LUMEN_REPLICAS` — but several things break quietly if you
turn it on:

| Concern | State at N processes | Severity |
|---|---|---|
| Prometheus `multiproc_dir` | Config key exists (`config.yaml.example:58`); **chart mounts no volume at it**, and nothing wipes it at startup, so a restarted pod's metrics are summed with the previous run's dead PIDs | **Blocks Phase 1** |
| New in-process gauges | `prometheus_client` gauges default to `multiprocess_mode='all'` → one series **per PID**, not a fleet number | **Blocks Phase 2** |
| Rate limiting | Already documented as requiring Redis (`chart/values.yaml:4`) — but the requirement is stated for **replicas**, and it applies equally to **processes** | Medium |
| Health checker | `lumen/__init__.py:470` gates on `BACKGROUND_WORKER`, which is a **process-wide env var**; uvicorn's children all inherit it, so it cannot mean "extra workers only". N processes ⇒ N× probe load on every backend every 60 s | Medium |
| `_rr_counters` round-robin (`lumen/services/llm.py`) | N independent rotations ⇒ uneven endpoint distribution | Low |
| `_get_request_rates` 30 s cache (`lumen/blueprints/api/routes.py`) | N caches ⇒ N× the `COUNT(*)` load | Low |
| Coin refiller | **Safe.** `lumen/services/token_refill.py:101-113` is a compare-and-set (`WHERE last_refill_at <= one_hour_ago`), so a second process's pass is a no-op | None |
| Config watcher | **Correct as-is.** Per-process reload is what you want | None |

**[DECISION]** "Scale beyond one process" is a prerequisite of this work, not a consequence of it.
Phase 1 exists to make the metrics that *already* exist true at N>1, before adding any new ones.

---

## 2. The decision that makes topology irrelevant

Three stores, one rule each. Every number produced by this work is assigned to exactly one, and the
assignment does not change with N or M.

### Store 1 — TimescaleDB (`request_logs`): everything historical, everything per-user

**Rule:** if the question contains a past tense or a user identity, it is answered by SQL over
`request_logs`.

**Why topology-invariant:** every process holds a connection to the same database. A row written by
process 7 of pod 3 is indistinguishable from any other. Adding processes adds write throughput
demand, not correctness risk.

**Covers:** TTFT history, queue-wait history, "how many distinct users were waiting for model X at
09:05" (interval-overlap `COUNT(DISTINCT entity_id)`), abort share, cost, per-user latency on
`/usage`. **This is the majority of the four asks.**

### Store 2 — Prometheus: everything aggregate, live, for machines

**Rule:** counters and histograms, labelled only by bounded-cardinality dimensions
(`model`, `source`, `reason`, `endpoint`). Never user identity.

**Why topology-invariant:** counters and histograms are additive. Within a pod, `prometheus_client`'s
multiprocess mode sums across processes (`lumen/blueprints/metrics/routes.py:182-200` already wires
`MultiProcessCollector`). Across pods, Prometheus sums across targets. **Provided** the plumbing in
Phase 1 is finished, N and M are free variables.

**Covers:** queue-wait distribution, queue depth, rejection rates, abort rates, latency histograms,
pool state, upstream gauges.

### Store 3 — `LiveState`: the three numbers that are live, fleet-wide, and needed *inside the app*

This is the entire topology-sensitive surface. It is small on purpose:

| Number | Why Prometheus cannot serve it | Why the DB cannot serve it |
|---|---|---|
| In-flight requests per model, right now | Could, in principle — but the in-app admin page has no Prometheus client and the chart ships no scrape config, so there is nothing to query | An in-flight request has no row; `request_logs.time` is the **completion** timestamp |
| **Unique users waiting per model, right now** | **Cannot.** Distinct-count over identities is exactly what Prometheus labels must never carry (§6 of the proposal) | Same — no row yet |
| Admission-control token per model (Phase 9 only) | Not a metric; a semaphore | Not a semaphore |

**[DECISION]** Put this behind `lumen/services/live_state.py`, with **at most two** implementations —
`LocalLiveState` (a dict + lock) and `RedisLiveState` — selected at startup by whether a Redis URL is
configured. Not three backends, not a plugin registry.

**Amended after review:** if Gate 0 finds production is 1 process × 1 replica, ship
`LocalLiveState` as **a plain class with no interface and no factory**, and introduce the abstraction
*together with* `RedisLiveState` when Phase 5 is actually unblocked. An interface with one
implementation is exactly the speculative generality CLAUDE.md §2 forbids, and "the seam makes
deferring free" is only true if the seam is free — it isn't if it is never used. Topology-invariance
survives this: `LocalLiveState` is correct at 1×1, and the call sites (`admit`/`release`) do not move
when the second implementation arrives.

**The honesty requirement.** Every live number rendered in the UI or served by
`/admin/api/status` carries the topology it was computed from, using the functions that already
exist:

```python
from lumen.services.db_pool import detect_workers, detect_replicas
# → {"scope": "local", "processes": 1, "replicas": 1}  or  {"scope": "fleet"}
```

so the tile reads *"12 users waiting (this process — 1 of 4 processes × 2 replicas)"* rather than
silently under-reporting by 8×. This is the single most important guard against the failure mode you
identified: a number that was correct under the defaults and became a lie under production config,
without anybody noticing.

---

## 3. Does this push us towards Redis?

**Partly — for one metric, one control, and one convenience. Not for "the instrumentation."**

### It does not push us to Redis for

- **History or per-user attribution** — Timescale, §1.1. Redis is not a time-series store, has no
  persistence in this chart, and putting the answer to "which students were rate-limited on Tuesday"
  in a cache that a node drain empties is not a design.
- **Rates, distributions, depths, saturation ratios** — Prometheus, which is additive across
  processes and replicas by construction. A queue-depth *gauge* summed over N processes with
  `multiprocess_mode='livesum'` is the fleet number, with no Redis involved.
- **Anything on the token path.** Two Redis round-trips per request is acceptable; two per token is
  not (§14).

### It does push us to Redis for

1. **"Unique users waiting for model X, right now", fleet-wide.** This is the one number that is
   simultaneously live, identity-bearing, and cross-process. Prometheus structurally cannot hold it;
   the database does not have the rows yet. A Redis `SADD`/`SREM`/`SCARD` per model — two operations
   per request, off the token path, TTL'd above the gateway budget so a killed process cannot inflate
   it forever — is the correct tool, and it is the *only* new thing in this plan that Redis is
   uniquely good at.
2. **Cross-fleet admission control (Phase 9).** A per-model concurrency cap that means anything at
   M replicas is a distributed semaphore. If admission control is adopted, Redis is required. If it
   is not adopted, this reason evaporates.
3. **Convenience: the in-app `/admin/status` live tiles being fleet-wide rather than per-process.**
   The alternative — have Lumen scrape its own Prometheus — trades a Redis dependency for a Prometheus
   dependency that the chart does not currently provision at all (**[FACT]** no `ServiceMonitor`, no
   `PodMonitor`, no `prometheus.io/*` annotations anywhere under `chart/`).

### And the cost is bounded by construction

**[DECISION]** Redis is never on the critical path of a proxied request. Encoded as five rules,
each with a test in §14:

- **Optional at import.** No Redis URL ⇒ `LocalLiveState`, logged once at INFO. No `redis` import at
  module scope in any request-path module.
- **Fail-open at call.** Every operation wrapped, `socket_timeout=0.25`, failure falls back to the
  local value and increments `lumen_live_state_errors_total{op}`. A Redis outage degrades the *admin
  page*, never a `/v1/chat/completions`.
- **Bounded call count.** At most two operations per request. Never per chunk.
- **Self-healing.** Per-request members carry a TTL above `gateway.timeout` (600 s), so a SIGKILL
  mid-request cannot leave a permanently inflated set.
- **Honest in the UI.** When `LocalLiveState` is active, the topology label says so.

**The scheduling answer:** Redis is *already required* at replicas ≥ 2 for rate limiting
(`chart/values.yaml:4`) and, per §1.3, at processes ≥ 2 as well. So if Gate 0 finds production is
already multi-process or multi-replica, **Redis is already deployed and this is not a new
dependency** — Phase 5 is then a small addition to something running. If Gate 0 finds production is
genuinely 1×1, Phase 5 is deferred and `LocalLiveState` is exactly correct. **Either way the plan
does not change shape**, which is the point of the seam in §2.

---

## 4. Gate 0 — ground truth before code

No code. These five answers change effort allocation, and four of them cannot be derived from the
repository. Everything in Phases 1–4 is safe to start in parallel with this; Phase 5 and Phase 8 are
blocked on it.

| # | Question | How to get it | What it decides |
|---|---|---|---|
| G0.1 | **Actual production topology** — replicas, `--workers`/`WEB_CONCURRENCY`, `LUMEN_WSGI_WORKERS` | `kubectl get deploy -o yaml`; `GET /metrics/debug` already prints workers × replicas and live `WSGI_*` thread count (`lumen/blueprints/metrics/routes.py:_format_deployment`) | Whether Phase 5 ships now or is deferred; whether Phase 1 is urgent or merely correct |
| G0.2 | **Is Redis deployed?** `redis.enabled`, or an external `redis.url` | Rendered values / `config.yaml` `rate_limiting.storage_url` | Whether Phase 5 is "add two calls" or "provision infrastructure" |
| G0.3 | **Timescale version, and is `timescaledb_toolkit` installed?** | `SELECT extversion FROM pg_extension WHERE extname LIKE 'timescale%'` | Exact p95 (`percentile_agg`) vs hand-rolled bucket counts; hierarchical continuous aggregates in Phase 8 |
| G0.4 | **Current size and growth of `request_logs`** | `SELECT pg_total_relation_size('request_logs'), count(*), min(time), max(time) FROM request_logs` | Replaces every storage estimate in proposal §9 with a measurement; sets the retention window |
| G0.5 | **Does a Prometheus/Grafana stack scrape this cluster, and at what interval?** | Ask the operators | Whether `/admin/status` is the primary surface or a convenience; whether Phase 1's ServiceMonitor is the deliverable |

**[GATE] G0** — the five answers are written into this document before Phase 5 or Phase 8 begins.
G0.1 and G0.2 are the blocking pair.

---

## 5. Phase 1 — multi-process integrity

*Make the metrics that already exist true at N processes × M replicas. No new metrics.*

This phase is what earns the right to say the system "supports scaling beyond one process". It is
small and it is a prerequisite for everything after it.

### Changes

1. **Chart: mount an `emptyDir` at `api.prometheus.multiproc_dir`** and set it by default when
   `wsgiProcesses > 1`. Add `wsgiProcesses` to `chart/values.yaml` + `chart/values.schema.json` (CLAUDE.md §5),
   wire it to both `WEB_CONCURRENCY` (so `db_pool.detect_workers()` and uvicorn agree on one number)
   and `uvicorn --workers`.
2. **`entrypoint.sh`: wipe the multiproc dir before `exec uvicorn`.** Stale `*.db` files from a
   previous run's PIDs are summed into every counter otherwise — a restarted pod reports lifetime
   totals from two lives.
3. **Reap dead PIDs, do not merely mark them on clean shutdown.** Registering
   `prometheus_client.multiprocess.mark_process_dead` at shutdown handles SIGTERM and nothing else —
   **a worker killed by SIGKILL (uvicorn's post-grace-period kill, or the OOM killer) never runs it**,
   and its gauge file keeps contributing its last value to every `livesum` aggregate for the rest of
   the **pod's** lifetime, because the dir wipe in item 2 runs once per pod start, not per worker
   respawn. A worker that dies holding `queue_depth=5` leaves the fleet depth 5 too high
   indefinitely — during a burst, which is when workers are most likely to be OOM-killed and when the
   gauge matters most. **Add a reconciliation pass**: scan the multiproc dir for PID-suffixed files
   whose PID is no longer alive (`os.kill(pid, 0)`) and `mark_process_dead` them. Run it at worker
   startup **and** on each scrape (a directory listing plus a handful of signal-0 probes), and
   **before** `MultiProcessCollector(...)` is constructed in
   `lumen/blueprints/metrics/routes.py:192`, not after.

   **Wrap every `mark_process_dead` call.** **[FACT]** the library's implementation
   (`prometheus_client/multiprocess.py:177-183`) does an unguarded `glob` + `os.remove` with no
   `try`/`except`. Two processes reaping the same dead PID concurrently means the loser raises
   `FileNotFoundError` **into the scrape** — an unhandled 500 on `/metrics`. With N workers plus a
   ServiceMonitor scraping every pod, concurrent reaps are routine, and they cluster right after an
   OOM kill: the scrape breaks exactly when a worker just died. Catch `(FileNotFoundError, OSError)`
   per call, or serialise the reap behind the same non-blocking flock as item 7.

   **PID reuse is worse than "a stale file survives".** Files are named `gauge_{mode}_{pid}.db`, so a
   respawned worker that draws the same PID opens the **same mmap** and inherits the dead worker's
   values — a worker killed holding `queue_depth=5` gives its successor a permanent +5 offset on
   every `inc`/`dec`. An mtime cross-check does not help, because the new process rewrites mtime.
   Mitigate by having each worker unlink **its own** PID's files at startup, before touching any
   metric.
4. **Document the invariant at the definition site** in `lumen/blueprints/metrics/middleware.py`: every `Gauge` added from
   here on declares an explicit `multiprocess_mode`; `Counter`/`Histogram` need nothing. State the
   asymmetry explicitly so nobody "fixes" it later: **dead PIDs' counter files are summed, and that
   is correct** — those increments are real history. Only `livesum`/`liveall` gauges must exclude
   them. The two cases look alike and the wrong fix silently loses counts.
5. **Fix `_normalize_path` cardinality** — label by the matched Flask url_rule with a single
   `"<unmatched>"` bucket. **[FACT]** Today it only collapses `/\d+`, so a scanner hitting `/.env`,
   `/wp-admin`, … mints an unbounded set of label values in every process's memory *and* in the TSDB.
   This gets N× worse with N processes and is the one pre-existing bug that Phase 1 makes urgent.

   **This is not a one-line change to `_normalize_path`, and the obvious fix does not work.** The
   middleware captures `path` *before* calling `wsgi_app`, when routing has not happened yet — but
   `request.url_rule` is **also unavailable after `wsgi_app` returns**, because Flask's `wsgi_app`
   runs `ctx.pop(error)` in its `finally` before returning (verified against the installed Flask).
   The label closures therefore run with no request context at all. **Correct approach:** stash
   `environ["lumen.url_rule"] = request.url_rule.rule if request.url_rule else None` while the
   context is still current, and read it from `environ` in the counter and latency closures, falling
   back to `"<unmatched>"`. Keep raw `_normalize_path` for the context-anomaly log messages, which
   want the real path.

   **Use `teardown_request`, not `after_request`.** `after_request` is skipped when a non-`Exception`
   `BaseException` unwinds the request; `teardown_request` runs regardless. The middleware's existing
   `except BaseException` path plus the `"<unmatched>"` fallback would cover the gap, but there is no
   reason to leave one.

   **Bound `method` in the same change — the label set has three dimensions, not one.**
   **[FACT]** `_http_requests` is labelled `["method", "path_template", "status"]`
   (`lumen/blueprints/metrics/middleware.py:22-26`) and `method` comes straight from
   `environ["REQUEST_METHOD"]` (`:117`). HTTP methods are RFC token grammar, so a scanner sending
   `PROPFIND`, `TRACK`, `FOOBARBAZ`, … mints a new series per method — the same unbounded growth,
   the same N× amplification across processes. Allow-list against the standard methods with an
   `"<other>"` bucket. **The Phase 1 exit gate must scan junk *methods* as well as junk paths**,
   otherwise it passes while the bug it exists to close stays open through the other dimension.
6. **Chart: a `ServiceMonitor`** (guarded by `serviceMonitor.enabled`, default false), scraping every
   pod. Multi-replica aggregation is Prometheus's job; it cannot do it if nothing is scraped.
7. **Elect a single health-probe runner.** **[FACT]** `lumen/services/health.py:27` holds a module-global
   `ThreadPoolExecutor(max_workers=8)` — one **per process** — and `BACKGROUND_WORKER`
   (`lumen/__init__.py:470`) is a process-wide env var that uvicorn's children all inherit, so it
   cannot mean "extra workers only". At `wsgiProcesses=4` with 10 endpoints that is 40 probes across
   32 threads every 60 s, aimed at the same GPU servers the students are queued behind, and the
   probes compete with real traffic precisely during a burst.

   **[DECISION] Elect, don't tolerate.** An earlier draft said "accept and document"; that was the
   wrong call once the per-process executor was accounted for. A `fcntl.flock` on a well-known file
   at the top of the probe pass is ~5 lines, needs no election service and no Redis, and is
   **naturally scoped to the pod** (a shared `emptyDir`), which is exactly the right granularity —
   one probe pass per pod, every pod probing. Non-holders skip the pass and read the DB result the
   holder wrote.

   **The lock must be non-blocking.** `fcntl.LOCK_EX | fcntl.LOCK_NB` — try, and skip the pass if
   someone holds it. A *blocking* flock would be strictly worse than the problem it solves:
   `lumen/services/health.py:81` commits inside the pass, that commit can block up to `pool_timeout` on an exhausted
   pool, and every other process would then queue behind a hung holder — stalling all health probing
   during exactly the burst when health data matters.

   **Do not add a "deadline after which the holder releases".** An earlier draft did; it recreates
   the problem. Nothing in the pass is preemptible — `future.result(timeout=_PROBE_TIMEOUT)`
   (`lumen/services/health.py:60`) is serial and legitimately runs 10 s × N endpoints,
   `_probe_executor` abandons stuck threads rather than killing them (documented at `:22-25`), and a
   blocked `commit()` cannot be interrupted from another thread. A watchdog could only unlock the
   flock **while the holder is still running**, admitting a second concurrent pass against the same
   GPU endpoints and raising the odds of the `StaleDataError` already handled at `:82`.
   **Instead: bound the pass by construction** (the probes already are; give the `commit()` a short
   statement timeout) and use **lock-file mtime staleness** — a non-holder that finds the heartbeat
   older than 3× the interval takes over.

   **Two mechanical details that decide whether it works:**
   - **Open mode.** Opening the lock file `"w"` truncates *before* the `flock` attempt, so every
     non-holder's failed attempt wipes the holder's heartbeat. Use `os.open(..., O_CREAT | O_RDWR)`
     or `"a+"`.
   - **Path.** Use a fixed container path (`/tmp/lumen-health.lock`), **not** the multiproc emptyDir:
     that volume only exists when `multiprocDir` is configured, and all uvicorn workers share one
     container anyway — a plain container path is already pod-scoped, so the emptyDir buys nothing.

### Tests

| Test | File | Asserts |
|---|---|---|
| Multiproc aggregation | `tests/unit/test_metrics_multiprocess.py` (new) | With `PROMETHEUS_MULTIPROC_DIR` set to a tmpdir, two child processes each increment `lumen_http_requests_total`; a third process's `MultiProcessCollector` reports the **sum** |
| Stale-PID hygiene | same | A dead PID's counter file still sums (correct for counters); after `mark_process_dead`, its `livesum` gauge file does not |
| Startup wipe | `tests/unit/test_entrypoint_multiproc.py` (new) | `entrypoint.sh` removes `*.db` under the dir before exec (shell-level assertion, or a Python re-implementation of the same guard) |
| Gauge mode guard | `tests/unit/test_metrics_middleware.py` (extend) | Static check: every `Gauge(...)` constructed in `lumen/` passes `multiprocess_mode` — same static-analysis shape as the existing `tests/unit/test_no_stream_with_context.py` |
| Path cardinality | `tests/unit/test_metrics_middleware.py` (extend) | 50 requests to distinct unmatched paths produce **one** `path_template` label value |
| Chart render | `tests/unit/test_chart_values.py` (new) | `helm template` with `wsgiProcesses: 4` renders the volume, the mount, `WEB_CONCURRENCY=4`; `chart/values.schema.json` accepts it |

### **[GATE] Phase 1 exit**

- `helm template --set wsgiProcesses=4` renders a pod that, when run, reports **one** set of HTTP
  counters equal to the sum of its four processes — verified by hand against a local 4-worker run.
- A pod restart does not increase any counter's reported total.
- `/metrics` label-value count for `path_template` is bounded by the number of Flask url_rules,
  proven by scanning a running instance with 100 junk paths.
- **Until this gate passes, `wsgiProcesses > 1` is not a supported configuration** and should be
  documented as such.

---

## 6. Phase 2 — the missing queue numbers

*Close the Q1 blind spot. Correct at N processes from the first commit.*

This is the highest value-per-line change in the whole plan and it is unchanged by the topology
argument — the queue is per-process, and per-process queues sum.

### Changes

1. **Stamp T0** — `time.monotonic()` **and** a wall-clock `started_at` into `environ` in
   `_DisconnectAwareWSGIResponder.__call__` (`lumen/services/wsgi_disconnect.py`), immediately before
   `loop.run_in_executor(...)`. **Read T1** at the top of the WSGI call in the worker thread.
   `local_queue_wait = T1 − T0`.

   **[DECISION] The capture must not live in the Prometheus middleware.** **[FACT]**
   `lumen/__init__.py:99-104` installs `make_metrics_middleware` **only when
   `api.prometheus.enabled`**, and the chart default is `enabled: false`
   (`chart/values.yaml:139-141`). Capturing T0/T1 there would leave `queue_wait`, `preflight` and
   `started_at` NULL on every row in the default deployment, making the headline historical query
   unanswerable for exactly the installations most likely to need it. T0 goes in
   `lumen/services/wsgi_disconnect.py` (always in the request path, via `asgi.py`) and T1 in a Flask
   `before_request` (always registered). Only the *histograms* live in `lumen/blueprints/metrics/middleware.py`.
2. **Explicit depth counters** around the submit — an `itertools.count`-free pair of `inc`/`dec` on a
   lock-free counter, **not** `executor._work_queue.qsize()` (private API, and it excludes the
   items already handed to threads).
3. **New metrics**, all in `lumen/blueprints/metrics/middleware.py` next to the existing three:
   - `lumen_wsgi_queue_wait_seconds` — Histogram, no labels, buckets `0.001 … 60`.
   - `lumen_wsgi_queue_depth` — Gauge, `multiprocess_mode='livesum'`.
   - `lumen_wsgi_threads_busy` / `lumen_wsgi_threads_total` — Gauges, `livesum` / `livesum`.
   - `lumen_rejections_total{reason, source, model}` with
     `reason ∈ {rate_limit, coin_budget, no_access, needs_consent, no_healthy_endpoint}`.
     `model` is empty for `rate_limit` — the body is unparsed at that point, and saying so is honest.
4. **Distinguish the two 429s.** Coin exhaustion gets `code: insufficient_quota` (matching the
   OpenAI taxonomy the API otherwise follows) instead of sharing `rate_limit_exceeded`. Add
   `Retry-After` to both paths — **[DECISION]** this is a genuine burst mitigation, not cosmetics:
   300 OpenAI-SDK clients given a bare 429 retry on independent schedules and can synchronise into a
   storm; the SDK honours `Retry-After`.
5. **Short-circuit already-disconnected queued work.** If the disconnect `Event` is set when the work
   item finally starts, return a 499-shaped response without running preflight, and count it as
   `lumen_rejections_total{reason="queue_shed"}`. **[FACT]** the disconnect pump is created *before*
   `run_in_executor`, so the flag is already accurate for queued requests — the information exists
   and is currently discarded.
6. **Shed before the gateway does, and record it.** A request that waits 400 s in the Q1 queue and
   then streams for 200 s hits the 600 s `gateway.timeout` (`chart/values.yaml`) and is cut from
   outside. Lumen records **nothing** about why: the client vanishes, and it is indistinguishable
   from any other disconnect. At the start of the upstream call, compare elapsed time against the
   budget — **`gateway.timeout − (T2 − T0)`, i.e. minus `queue_wait` *and* `preflight`**, not
   `queue_wait` alone; under the burst that makes preflight large, omitting it hands the request a
   few seconds of budget that do not exist. If the budget is spent, fail fast with 429 +
   `Retry-After` rather than starting a generation that cannot be delivered.

   **The 429 must happen in the VIEW, not "at the start of the upstream call".** **[FACT]** On both
   streaming paths the upstream call lives inside a generator that does not begin executing until
   body iteration — by which time Flask has already called `start_response`, and a2wsgi has already
   queued `http.response.start` with **status 200** to the client
   (`a2wsgi/wsgi.py:244-256`; chat at `lumen/blueprints/chat/routes.py:247`, API at
   `lumen/blueprints/api/routes.py:584`). A check placed where an earlier draft said would leave only
   an in-band SSE `error` event on a 200 — which the OpenAI SDK does not treat as a retryable 429 and
   which carries no `Retry-After`, defeating the anti-synchronisation argument in item 4. **Put the
   admission check in the views, before the `Response(...)` is constructed**, using T1 (the value
   available there); state plainly that it therefore excludes the generator's own preflight.

   **Two cases, and be honest that this catches one of them.** The admission check catches requests
   whose budget is *already* spent at admission. A request that starts with 100 s of budget and then
   streams for 200 s is still cut mid-generation by the gateway, and Lumen's disconnect detection
   fires — so it records `disconnect`, which is exactly the conflation this item claims to fix.
   Closing that requires a **mid-stream deadline check** at the existing inter-chunk poll
   (`lumen/services/llm.py:807`, which already runs per chunk and costs nothing extra): when elapsed
   exceeds the budget, end the stream and record `outcome='timeout'` **before** the gateway's cut can
   be mistaken for a disconnect. Give it a **safety margin**: the check only runs *between* chunks
   (bounded by `LLM_READ_TIMEOUT`) and the gateway's clock starts before Lumen's T0, so a budget of
   exactly the gateway's own value loses the race and still records `disconnect`.

   **The budget is a new config key — it does not exist in the app today.** **[FACT]**
   `gateway.timeout` is a Helm value consumed only by `chart/templates/httproute.yaml`, guarded by
   `gateway.enabled` (default false), and it is never rendered into `config.yaml` — grep
   `chart/templates/` confirms no `gateway` key reaches the app. In the default ingress deployment
   the real cut comes from ingress annotations, so copying `gateway.timeout` would be wrong rather
   than merely missing. **Add an explicit `api.request_budget_seconds`**, hot-loaded (see
   `lumen/services/config_watcher.py` for the pattern), mirrored into `chart/values.yaml` and
   `values.schema.json` per CLAUDE.md §5, documented as "must match whatever fronts Lumen". Do not
   derive it from `gateway.timeout`.
7. **DB pool checkout-wait histogram.** `pool_tracker` already hooks SQLAlchemy's `checkout`/`checkin`
   events, so the wait is a subtraction away. Today the pool is observable only as a *depth*, so
   "the pool is full" and "the pool is full **and requests are queued behind it**" look identical.
   This lands here rather than later because §13a.1's write-spike measurement depends on it — that
   section referenced it as though it already existed.
8. **Load-test harness:** split TTFT from total elapsed in `loadtesting/locustfile.py`, and add a
   step load shape (0 → N in seconds). A ramp does not reproduce a class start.

### Tests

| Test | File | Asserts |
|---|---|---|
| Queue wait measured | `tests/unit/test_wsgi_queue_metrics.py` (new) | With `workers=1` and two concurrent requests where the first sleeps 200 ms, the second's observed `queue_wait` ≥ 150 ms and the first's ≈ 0 |
| Depth rises and returns | same | Depth gauge > 0 while requests are parked; **returns to exactly 0** after all complete — including the exception path |
| Depth is decremented on every exit | same | Parametrised over: normal return, view raises, `_StalledClient`, client disconnect mid-stream. This is the leak-prone part |
| Multi-process sum | `tests/unit/test_metrics_multiprocess.py` (extend) | Two processes each with depth 3 report `lumen_wsgi_queue_depth == 6` under `livesum` |
| Shed on pre-known disconnect | `tests/integration/test_disconnect.py` (extend) | A request whose client disconnects while queued never reaches the view; the counter increments |
| Rejection taxonomy | `tests/routes/test_metrics_routes.py` (extend) | Over-limit ⇒ `reason="rate_limit"` + `Retry-After`; zero-coin ⇒ `reason="coin_budget"` + `insufficient_quota` + `Retry-After`; the two are distinguishable in both body and metric |
| No token-path cost | `tests/unit/test_llm_functions.py` (extend) | Streaming 500 chunks performs **zero** `Histogram.observe` calls (assert via a patched observe) |

### **[GATE] Phase 2 exit**

- Locust step load, 300 users, dummy backend, `wsgiWorkers=10`: `lumen_wsgi_queue_depth` peaks near
  290, `queue_wait` p95 tracks it, **both return to zero** within seconds of the run ending. A depth
  gauge that does not return to zero is a leak and blocks the phase.
- The same run at `wsgiProcesses=4` reports a single fleet depth, not four series.
- Run the burst twice in the same process; the second run's baseline is 0, not the first run's peak.

---

## 7. Phase 3 — schema and a database CI

*The columns that unlock every retrospective question — and the CI that can actually test them.*

**[FACT] The prerequisite is real.** `tests/conftest.py:34` builds the schema with `db.create_all()`
on SQLite, `.github/workflows/test.yml` provisions **no services**, and `tests/unit/test_migrations.py`
only checks the Alembic graph shape without a database. **Nothing in the suite has ever executed the
hypertable, the continuous aggregate, or a single line of the `/api/usage/*` SQL** — those endpoints
return empty on the dialect check before reaching the query. Anything aggregate-shaped built without
fixing this ships untested.

### Changes

1. **CI service container.** Add `timescale/timescaledb:latest-pg17` as a service to
   `.github/workflows/test.yml`, plus a `postgres` pytest marker and a session fixture that runs
   `flask db upgrade` (not `create_all`) against it. Existing SQLite tests are untouched and still
   run; the new marker is additive.
2. **New nullable columns on `request_logs`**, with column comments (CLAUDE.md §5):
   - `queue_wait` (Float) — T1 − T0, Lumen's own admission wait.
   - `ttft` (Float) — first chunk of **any** kind, including reasoning deltas.
   - `ttft_visible` (Float) — first *content* delta; today's `t_first`. Two columns, because on a
     reasoning model these differ by tens of seconds and conflating them makes a thinking model look
     like a queued one.
   - `send_blocked` (Float) — accumulated time blocked handing chunks to the server. Separates
     "slow client" from "slow backend", which `duration` currently conflates.
   - `outcome` (String(16)) — **enum limited to what can actually be written: `ok` and `disconnect`.**

     **[FACT] An earlier draft listed six values, four of which are unwritable, and defended
     `billing_error` on reasoning that is exactly backwards.** `request_logs` rows are only ever
     created inside `update_stats`, which ends at `db.session.flush()`
     (`lumen/services/llm.py:565-568`) — the `commit()` belongs to the caller. So:
     - **`billing_error` is unreachable.** The failing commit is the one that would have persisted
       the row; the rollback takes the flushed `RequestLog` with it
       (`lumen/services/llm.py:845-853`, `lumen/blueprints/api/routes.py:530-537`). The counter fires
       and the row does not exist. Including the value would *guarantee* the disagreement with the
       metric that including it was meant to prevent.
     - **`upstream_error` is unreachable.** Those paths return or re-raise before any `update_stats`
       (`lumen/blueprints/api/routes.py:378-380`, `lumen/services/llm.py:860-871`).
     - **`stalled_client` is indistinguishable from `disconnect`.** `_StalledClient` unwinds through
       a2wsgi's `finally: iterable.close()` into `GeneratorExit` → `_abort()` →
       `record_stream_abort(...)` with the default `reason="disconnect"`, and `send` has already
       called `self.disconnected.set()`. `record_stream_abort`'s own docstring says "nothing
       distinguishable is available at this seam."
     - **`timeout`** depends on §6 item 6 shipping the mid-stream check; add the value in that change,
       not this one.

     **[DECISION]** Ship `ok` and `disconnect`. Any further value must arrive together with the code
     that writes it. Making `upstream_error`/`billing_error` real requires a **best-effort insert on a
     fresh session after rollback**, which is new failure-path work and breaks the "statement count
     unchanged" property below — price it separately or not at all. `stalled_client` requires the
     responder to publish a distinct flag through a shared mutable holder (`_StalledClient` is caught
     in `__call__`, not in the generator, so an exception type cannot carry it).

   Nullable **with a server default**, per the precedent documented in
   `migrations/versions/e6f7a8b9c0d1_add_request_logs_aborted.py` — Timescale rejects propagating a non-defaulted NOT NULL
   column to populated chunks. **No backfill**, with the reason in the migration docstring.
   **Defaults, stated rather than left to the implementer:** `server_default='0'` on every Float
   column; `outcome` nullable with **no** default, where NULL means "written before this migration,
   unknown" — a default of `'ok'` would silently assert success about rows nobody measured.
   `send_blocked` is `0.0` on non-streaming paths (there is no send loop to block in), not NULL —
   NULL there would be indistinguishable from "not measured".
3. **Populate on all four paths** — chat stream, API stream, API non-stream, audio. TTFT is chat-only
   today, so the entire `/v1` surface is currently a latency blind spot.
4. **Composite index `(model_config_id, time DESC)`** — the access pattern every operator query uses.
5. **`lumen_llm_ttft_seconds{model, source}`** and `lumen_llm_duration_seconds{model, source, stream}`
   histograms, observed **once per request** at the end.
6. **`docs/dbschema.md`** updated (CLAUDE.md §5). **`CHANGELOG.md`** unreleased section.
7. **Do not touch `request_logs.time`** — it stays the completion timestamp and the partition key.
   Redefining it would silently shift every existing chart.
8. **`started_at` (TIMESTAMPTZ) — store the absolute start, do not derive it.**

   **[FACT] An earlier draft of this plan derived start as `time − duration − queue_wait`. That is
   wrong**, and wrong in the direction that matters. Verified in `lumen/services/llm.py`:
   - `t0 = time.time()` is set at `:752`, **after** the `with app.app_context():` preflight block
     (`:725-750`) that does the model lookup, endpoint selection, cache-salt derivation and timeout
     resolution. So `duration = T5 − T2`, excluding all preflight.
   - `RequestLog.time = datetime.now(timezone.utc)` is stamped inside `update_stats` (`:554-555`),
     called at `:848` **after** `duration` was computed (`:832`) and **after** `subtract_coins`.
     So `time ≈ T5 + billing_delay`.

   Therefore `time − duration − queue_wait = T0 + preflight + billing_delay`, **not `T0`.** The
   derived "waiting" interval is shifted right and shortened by the two unmeasured gaps — and
   `preflight` contains a DB pool checkout bounded by `pool_timeout` (10 s), so **the error is
   largest exactly during the burst this plan exists to diagnose.** The same gap makes the
   `user_perceived_wait = T4c − T0` SLI uncomputable from the stored columns.

   **[DECISION]** Store `started_at` as an absolute timestamp stamped at T0 and carried through on
   the same INSERT. The waiting interval becomes
   `[started_at, started_at + queue_wait + preflight + ttft_visible]` — a direct comparison, no
   reconstruction, no unmeasured gaps. One more column on a statement that already runs.

   **Type note:** `started_at` is `TIMESTAMPTZ`, matching `time` rather than the naive-UTC
   convention in CLAUDE.md §5. This is a deliberate, documented exception: the column exists to be
   compared and subtracted against `time` **on the same row**, and mixing naive and aware timestamps
   in that arithmetic is a Postgres footgun. Record the reason in the column comment and in
   `docs/dbschema.md`.
9. **Also store `preflight` (T2 − T1).** It is the only remaining unmeasured span in the request, it
   is where DB pool contention shows up, and without it "the queue was fine and the model was fine
   but requests were still slow" has no explanation.

### Implementation contract — decisions this plan makes so nobody has to stop and ask

These five were found by round-2 review as places an engineer would have to invent a design. They
are decided here.

**(a) One clock. All spans are `time.monotonic()` — and the conversion is SIX sites, not four.**
**[FACT]** T2 today is `t0 = time.time()` (`lumen/services/llm.py:752`, wall-clock; likewise
`lumen/blueprints/api/routes.py:368,462,641`), while this plan stamps T0/T1 with `time.monotonic()`.
**Subtracting them is meaningless**, and `queue_wait + preflight + ttft_visible` would mix three
clocks. §14's "monotonic for durations" is therefore **not** a review-checklist aspiration — it is a
required Phase 3 change.

**[FACT] The dangerous part is that `t0` escapes its function.** An earlier draft scoped the
conversion to "the four paths' own `duration` lines". That is wrong and would have shipped a
catastrophe. `record_stream_abort` computes its own duration **from the caller's `t0`**:

```
lumen/services/llm.py:686     duration=time.time() - started_at     # started_at IS the caller's t0
lumen/services/llm.py:772     started_at=t0                          # chat stream
lumen/blueprints/api/routes.py:475   started_at=t0                   # API stream
```

Convert `t0` to monotonic without touching `:686` and **every aborted row stores
`duration ≈ 1.76e9`** — about 55 years. That is the exact row set this plan exists to study
(disconnects during a class-start burst), it poisons `lumen_llm_duration_seconds` permanently into
`+Inf`, it makes any `AVG(duration)` on `/usage` garbage the moment one student closes a tab, and
**nothing raises**. The same applies to `t_first = time.time() - t0` (`llm.py:819`), which is
user-visible: it flows to `Message.time_to_first_token` and is rendered in the chat UI, and §7 item 2
defines `ttft_visible` as "today's `t_first`".

**Enumerate all six, convert all six:** `lumen/services/llm.py:686`, `:819`, `:832`;
`lumen/blueprints/api/routes.py:377`, `:523`, `:649`. Only `started_at` stays wall-clock, because it
is a stored instant rather than a span.

**Guard it with a static test** in the shape of `tests/unit/test_no_stream_with_context.py`: no
`time.time()` may appear in a subtraction anywhere in `lumen/services/llm.py` or
`lumen/blueprints/api/routes.py`. A grep-shaped invariant is the only thing that stops the seventh
site being added later.

**(b) `started_at` is stamped with `datetime.now(timezone.utc)`, not `utcnow()`.** CLAUDE.md §5
mandates `lumen.timeutils.utcnow()`, which returns **naive** UTC. Writing naive into a `TIMESTAMPTZ`
column makes Postgres interpret it against the session `TimeZone` — a silent, deployment-dependent
offset bug. `request_logs.time` already does the right thing (`lumen/services/llm.py:555`); `started_at` follows it
for the same reason and the same documented exception.

**(c) Threading the values into the generators — including the abort path.** `update_stats` is
called from five sites, **two of which are context-free streaming generators**
(`lumen/services/llm.py:848`, `lumen/blueprints/api/routes.py:533`) that must never touch `request`.
The pattern already exists and is documented: `client_disconnect_event()`
(`lumen/services/wsgi_disconnect.py:338-352`) is called in the view while the context is live and
captured into the generator's closure. Do exactly that — read `started_at`/`queue_wait` from
`request.environ` in the view, pass them as parameters into
`send_message_stream`/`_do_chat`/`_do_audio`, and add them to the `update_stats` signature.

**The fifth call site is the one that matters most and an earlier draft omitted it.**
`update_stats` is also reached at `lumen/services/llm.py:641` inside `record_aborted_request`, via
`record_stream_abort` (`:684`). **Both signatures must be threaded too.** Miss them and every
*aborted* row gets `started_at`/`queue_wait` NULL — the disconnect rows the headline "who was
waiting" query needs most, and the ones §6 item 5 and the abort-share SLI are about.

**Absent-key behaviour, stated to resolve a contradiction.** Falling back to `None` keeps the
Werkzeug dev server, the Flask test client and direct unit-test calls working (same reasoning as
`client_disconnect_event`'s docstring) — but note two consequences an earlier draft did not
reconcile:
- `None` inserts SQL NULL and **bypasses** the `server_default='0'` above. That is correct — "not
  measured" must stay distinguishable from "zero wait" — but it must be deliberate.
- It makes the Phase 3 test "all four paths write non-null `queue_wait`" **unsatisfiable under
  `test_client`**, which bypasses `asgi.py` entirely so T0 is never stamped. Move that assertion to
  the real-uvicorn harness (`tests/integration/test_disconnect.py:205-226`); the `test_client` suites
  assert `ttft`/`outcome` only.

**(d) Rename to avoid a collision — at BOTH call sites.**
`record_stream_abort(..., started_at=...)` (`lumen/services/llm.py:654`) already uses `started_at` to
mean **T2**. Rename it to `stream_t0`; two meanings of `started_at` one function apart is a bug
waiting to be written. It has **two** callers — `lumen/services/llm.py:772` **and**
`lumen/blueprints/api/routes.py:475`. Renaming only the first leaves the second raising `TypeError`
*inside* `_abort()`, which runs from the `except GeneratorExit` handler — so the `TypeError` replaces
the `GeneratorExit`, Python raises `RuntimeError: generator ignored GeneratorExit`, and **the abort
is never billed**. Only caught by tests if the API stream path is covered in
`tests/integration/test_disconnect.py`; check that it is.

**(e) `send_blocked` needs a mechanism, and the obvious one is wrong three ways.** **[FACT]**
`_DisconnectAwareWSGIResponder.send` (`lumen/services/wsgi_disconnect.py:241-269`) calls
`future.result(self.send_timeout)` and accumulates nothing, so the column as specified would be
permanently 0. The naive fix — "time each `future.result()`, publish into `environ`, read at billing
time" — fails on all three counts:

1. **It records zero for the stalled client.** The largest block of the request's life happens on the
   `except concurrent.futures.TimeoutError` branch, which raises `_StalledClient` without
   accumulating. So the column reads ~0 for exactly the outcome it exists to identify. **Accumulate
   in a `finally` around `future.result()`, so the timeout branch is counted.**
2. **The view cannot read it.** Billing on both streaming paths happens **in the generator**
   (`lumen/services/llm.py:846-853`, `lumen/blueprints/api/routes.py:531-536`), and at view time
   nothing has been sent yet — a value captured into the closure per contract (c) is `0.0` forever.
   **Capture a mutable holder** (a small dataclass or one-element list) published in `environ`
   alongside `ENVIRON_KEY`, so the generator reads the live total at billing time. Note the responder
   and the generator are on the *same* worker thread (`send` runs inside `run_in_executor`), so this
   is a timing problem, not a threading one.
3. **It excludes the tail.** Billing precedes the final chunks and `data: [DONE]`, by design, so the
   recorded value always omits them. Say so in the column comment.

**And be honest about what it measures:** time to enqueue onto a bounded `asyncio.Queue`, which also
absorbs event-loop scheduling latency. Under a 300-user burst that is *not* purely "slow client" —
which is what a naive column comment would claim. **If this lands after Phase 3, cut the column**
rather than shipping a permanently-zero one that reads as "no client backpressure ever".

**Per-path semantics of `preflight` — document, do not pretend uniformity.** It is *not* purely DB
contention on every path:
- **Audio** (`lumen/blueprints/api/routes.py:587-679`): includes `upload.read()` at `:605`, i.e. multipart parsing of
  the whole body, which for a large file dominates. High audio `preflight` is an upload, not a pool.
- **Chat stream**: there are effectively *two* preflights — the view's (`lumen/blueprints/chat/routes.py:213-237`) and
  the generator's second lookup (`lumen/services/llm.py:725-750`) — and `T2 − T1` spans both plus the handoff.
Record this in the column comment and in `docs/dbschema.md`, or an operator will read audio latency
as database pressure.

### Tests

| Test | File | Asserts |
|---|---|---|
| Migration round-trip on Timescale | `tests/integration/test_migrations_postgres.py` (new, `@pytest.mark.postgres`) | `upgrade` then `downgrade` clean against a populated hypertable; the new columns propagate to existing chunks |
| Hypertable still a hypertable | same | `timescaledb_information.hypertables` still lists `request_logs` after the migration |
| Continuous aggregate intact | same | `request_counts_hourly` refreshes and returns rows after the column addition |
| All four paths populate | `tests/routes/test_api_*.py`, `tests/routes/test_chat_routes.py` (extend) | Each path writes a row with non-null `ttft`, `ttft_visible`, `queue_wait`, `outcome` |
| Reasoning split | `tests/unit/test_llm_functions.py` (new case) | A stream of 3 reasoning deltas then content sets `ttft` at the first reasoning delta and `ttft_visible` at the content delta; `ttft < ttft_visible` |
| Abort sets outcome | `tests/integration/test_disconnect.py` (extend) | A mid-stream disconnect writes `outcome='disconnect'`, `aborted=True`, and a non-null `ttft` |
| The headline query | `tests/integration/test_usage_queries_postgres.py` (new) | Against seeded data, "distinct users waiting for model X at instant T" returns the hand-computed answer |
| **`started_at` is really T0** | `tests/integration/test_started_at_accuracy.py` (new) | A **real request through the full stack**, with T0 independently recorded by a test-only hook, asserts `\|started_at − recorded_T0\| < 0.1 s`. **Without this the seeded-data test above passes vacuously** — it proves the SQL is right, never that the data is. This is the test that would have caught the derivation bug. **Harness — reuse, do not invent:** Flask's `test_client()` bypasses `asgi.py` entirely, so T0 would never be stamped and the test would assert nothing. `tests/integration/test_disconnect.py:205-226` **already solves this**: it imports `asgi` with `lumen.create_app` patched to return the test app, then runs **real uvicorn** on an ephemeral port, deliberately so the test exercises the wiring production uses rather than a copy that can drift. Reuse that fixture and record the independent T0 from a test-only callback at the same point in `_DisconnectAwareWSGIResponder.__call__` |
| Preflight is non-zero and bounded | same | A request whose preflight includes a contended pool checkout records `preflight > 0`; `started_at + preflight + ttft ≈ first byte observed by the client` |
| Statement count unchanged | `tests/unit/test_llm_functions.py` (extend) | `update_stats` issues the **same number** of statements as before the change (columns ride the existing INSERT) |

### **[GATE] Phase 3 exit**

- CI is green with the Postgres+Timescale service, and **at least one test fails if the hypertable is
  replaced by a plain table** — proof the new suite actually exercises Timescale rather than passing
  vacuously.
- `flask db upgrade` → `downgrade` → `upgrade` on a database seeded with `seed_analytics.py` leaves
  `/usage` rendering identically.
- The added statement count per request is **zero**.

---

## 8. Phase 4 — decouple `/metrics`, introduce the LiveState seam

### Changes

1. **`LumenDBCollector` serves a background-refreshed snapshot.** **[FACT]** it currently queries the
   database on every scrape — a `GROUP BY` over `model_stats` (one row per entity × model × source)
   plus two `COUNT(*)` over `entities` — taking a pooled connection to do it. During the burst it
   competes for the pool it is reporting on and can block up to `pool_timeout`, so **`/metrics`
   degrades exactly when it is needed**. Refresh from a daemon thread on the same pattern as
   `lumen/services/health.py` every 30–60 s; `collect()` becomes a pure in-memory read.
   Export `lumen_metrics_snapshot_age_seconds` so staleness is visible rather than silent.
   *This is a cost reduction and it belongs early.*
2. **`lumen/services/live_state.py`** — the seam. Interface, deliberately minimal:
   ```python
   admit(model_key: str, entity_id: int) -> None
   release(model_key: str, entity_id: int) -> None
   snapshot() -> dict[str, ModelLive]     # inflight, unique_users
   topology() -> dict                     # scope, processes, replicas
   ```
   `LocalLiveState` only in this phase. Called exactly twice per request, from the same places that
   already own request lifecycle.
3. **Cache the `models_page` per-model counts.** **[FACT]** `lumen/blueprints/models_page/routes.py` runs two uncached
   `COUNT(*)` scans of `request_logs` per model-page view, while the API's equivalent is cached 30 s
   40 lines away in `lumen/blueprints/api/routes.py`. 300 students opening a model page during the incident is 600
   sequential scans of a growing hypertable at the worst possible moment. Copy the existing pattern,
   or read the snapshot.

### Tests

| Test | File | Asserts |
|---|---|---|
| Scrape issues no SQL | `tests/routes/test_metrics_routes.py` (extend) | 20 consecutive `/metrics` requests execute **zero** statements (SQLAlchemy event listener counting `before_cursor_execute`) |
| Snapshot age exported | same | Gauge present and increasing between refreshes |
| Refresher releases its session | `tests/unit/test_db_teardown.py` (extend) | After a refresh cycle, pool `checked_out == 0` — the 1.22.0 leak class, guarded |
| Stale snapshot degrades visibly | same | With the refresher stopped, the age gauge grows and the values do not silently change |
| LiveState balance | `tests/unit/test_live_state.py` (new) | `admit`/`release` parametrised over every exit path leaves `inflight == 0` and `unique_users == 0` |
| Topology honesty | same | `topology()["scope"] == "local"` with no Redis; `processes`/`replicas` reflect `WEB_CONCURRENCY`/`LUMEN_REPLICAS` |
| Model page cached | `tests/routes/test_models_routes.py` (extend) | Second page load within the TTL issues no new `COUNT(*)` |

### **[GATE] Phase 4 exit**

- Scraping `/metrics` at 1 s for 60 s during a Locust burst causes **no** measurable change in pool
  checkouts and no change in request latency p95.
- `LiveState` counters return to zero after a 300-user burst that includes disconnects, upstream
  errors and stalled clients.

---

## 9. Phase 5 — the Redis backend

**Blocked on [GATE] G0.1/G0.2.** If production is 1 process × 1 replica, this phase is **deferred,
not cancelled** — the seam from Phase 4 is what makes deferring it free.

### Changes

1. **`RedisLiveState`** behind the Phase 4 interface. Per model: a Redis SET of entity ids for
   in-flight, `SADD`/`SREM`/`SCARD`. One `INCR`/`DECR` for in-flight count, or derive it from the set.
   Pipelined into one round trip per call site ⇒ **two round trips per request**, both off the token
   path.
2. **Selection at startup**, from the Redis URL that rate limiting already resolves — no new config
   key, no second URL to keep in sync.
3. **Fail-open wrapper.** `socket_timeout=0.25`, `socket_connect_timeout=0.25`, every call inside
   `try/except`, failure ⇒ local value + `lumen_live_state_errors_total{op}`.
4. **TTL self-healing.** Members expire above `gateway.timeout` (600 s), so a SIGKILL mid-request
   cannot leave a set permanently inflated. A periodic reconciliation against local state, logged
   when it corrects anything.
5. **Chart:** document that `wsgiProcesses > 1` **or** `replicaCount > 1` requires Redis (the values
   comment currently says replicas only). Fix the three pre-existing Redis chart bugs found in the
   proposal's §8 while in the file — `existingSecret`/`existingSecretKey` read by no template, the
   `auth.existingSecret` password-less URL, and `auth.password` landing in plaintext in the rendered
   config Secret. Add the `rate_limiting.storage_url` omission from `RESTART_REQUIRED` in
   `lumen/services/config_watcher.py`.

### Tests

| Test | File | Asserts |
|---|---|---|
| Fleet-wide unique users | `tests/integration/test_live_state_redis.py` (new, `@pytest.mark.redis`) | Two processes admit overlapping entity sets; `SCARD` is the **union**, not the sum |
| Fail-open on outage | `tests/unit/test_live_state.py` (extend) | With a Redis client whose every call raises, `admit`/`release`/`snapshot` all succeed, the request completes, the error counter increments, `topology()["scope"] == "local"` |
| Fail-open on timeout | same | A client that sleeps 5 s does not add 5 s to the request — the wrapper's 0.25 s timeout bounds it |
| Bounded call count | same | One proxied request performs **≤ 2** Redis commands, asserted with a counting fake, including on the abort path |
| TTL set | same | Every key written carries a TTL > `gateway.timeout` |
| Reconciliation | same | An orphaned member (simulated killed process) is removed and the correction is logged |

### **[GATE] Phase 5 exit**

- With Redis stopped mid-burst, **no request fails and no latency percentile moves**; only the admin
  page degrades, and it says so.
- Two pods × two processes report one unique-user count equal to the true union, verified against
  `request_logs` after the run.

---

## 10. Phase 6 — upstream queue depth

**[FACT]** From Lumen's side, "queued behind 40 sequences" and "the model is slow" are the same
number. Only the backend can tell them apart. **[FACT]** `lumen/services/model_sync.py` already probes SGLang's
`/get_server_info` at the server root by stripping `/v1`, and already detects the backend type — but
**discards it**.

- Persist the detected `backend` type and the concurrency capacity (`--max-num-seqs` or the SGLang
  equivalent) on `model_endpoints`, so depth has a denominator.
- Background scraper per endpoint, modelled on `lumen/services/health.py` (bounded executor, hard deadline,
  best-effort), parsing a **configured allowlist** of gauge names per backend type into the Phase 4
  snapshot. Metric names are a moving target across vLLM versions — configuration, never constants,
  and an unknown name degrades to "unknown", never to a crash or a silently wrong gauge.
- Add health-probe **latency** to `lumen/services/health.py`, which currently records only a boolean — a backend
  whose probe latency has quadrupled is degrading before it flips unhealthy.
- Optionally also let Prometheus scrape the backends directly (a second ServiceMonitor). Free, and
  complementary: Grafana gets full resolution, Lumen gets the joinable in-app number.

**Tests:** allowlist parse against captured fixture payloads from both backend types; unknown metric
name ⇒ gauge absent and one warning, not an exception; scraper failure ⇒ stale-but-labelled values;
scraper thread releases its DB session (`tests/unit/test_db_teardown.py`).

**[GATE] Phase 6 exit** — saturating a real backend beyond `max_num_seqs` shows a non-zero waiting
gauge in Lumen that tracks the backend's own within one scrape interval. **[GATE] G0-adjacent:**
confirm backend `/metrics` is reachable and unauthenticated from the Lumen pod before writing the
scraper — vLLM's `--api-key` guards `/v1` but historically not `/metrics`, and network policy may
still block it.

---

## 11. Phase 7 — the views

**[DECISION] Conditional on Gate 0.5 — this phase may need to move to the front.** If G0.5 finds
there is **no Prometheus/Grafana stack scraping this cluster**, then every metric added in Phases 1–6
has no consumer, and the only operator surface in existence is the plain-text `/metrics/debug`. In
that case a bare-bones `/admin/status` — live tiles reading the Phase 4 snapshot, nothing historical
— should ship immediately after Phase 4, before Phases 5 and 6. Building six phases of Prometheus
instrumentation for a Prometheus that does not exist is the most expensive mistake available in this
plan, and G0.5 is a single question to the operators.

- **`/admin/status`**, reclaiming the dead `/admin/analytics` redirect slot (its 346-line orphaned
  template is a stale near-copy of `usage.html`). Live tiles from the Phase 4 snapshot + LiveState —
  **never** from the database — plus historical charts from the Phase 8 1-minute aggregate. Chart.js
  4 as everywhere else. Every live tile carries the `topology()` label.
- **Chat:** when nothing has arrived yet, an elapsed indicator and, if the backend reports a queue,
  "waiting for the model (N ahead of you)". This is the single change that most improves the
  class-start experience — a student who knows they are queued does not reload, and every reload is
  another rate-limit bucket entry and another thread.
- **Model detail:** "typical wait right now" (median TTFT, last 5 min) from the snapshot, replacing
  the two uncached `COUNT(*)` scans.
- **`/usage` single-user:** median and p95 TTFT and abort share — free from the Phase 3 columns, and
  it answers the question a student actually has.
- **Nav** in all four theme headers; consider a shared admin-nav partial while there.

**Accessibility (CLAUDE.md §6) — the non-obvious ones for this page:**
auto-refreshing numeric tiles are **`aria-live="off"`** with an explicit Refresh button, and a
*separate* small `role="status"` region announces only **state transitions** ("model X is now
queued"). A polite live region updating every 5 s is unusable with a screen reader. Every `<canvas>`
gets `role="img"` + `aria-label` + text fallback; every table a `<caption class="visually-hidden">`;
queue state never colour-only; timestamps as `<span class="local-datetime" data-utc="…Z">`.

**Tests:** `tests/ui/test_accessibility.py` extended to cover `/admin/status` (it already runs the
compliance sweep); `tests/routes/test_admin_routes.py` for `@admin_required` on both the page and
`/admin/api/status`, and that the JSON endpoint executes **zero** SQL statements; a snapshot test that
the topology label renders "this process" when `LocalLiveState` is active. Re-capture affected
screenshots in `docs/img/` and update the matching `docs/guides/` pages (CLAUDE.md §5).

---

## 12. Phase 8 — lifecycle: aggregates, compression, retention

**Blocked on [GATE] G0.3/G0.4.** **Order is not negotiable** — this is the most likely way the whole
effort causes a user-visible regression.

1. **Entity-dimensioned continuous aggregate first.** **[FACT]** `/usage`'s per-user charts read raw
   `request_logs` precisely because `request_counts_hourly` has no `entity_id` column. Enabling
   retention before this exists would **silently truncate every individual's "All Time" history while
   the org-wide charts kept going** — an asymmetry that would be reported as data loss.
2. **`request_metrics_1m`** — 1-minute buckets on `(model_config_id, source)` with
   `end_offset => 1 minute`, carrying counts, token sums, abort counts and duration/TTFT
   sums-and-maxima. The hourly aggregate lags by ≥ 1 hour and is useless for "what happened during
   the 9 a.m. lab". Build hierarchically on top of it **only if** G0.3 confirms the Timescale version
   supports it.
3. **Then compression.** `add_compression_policy('request_logs', INTERVAL '7 days')`,
   `segmentby = model_config_id, source`, `orderby = time DESC`. Chunks are already 7 days, so it
   aligns naturally.
4. **Then retention.** **[DECISION] 13 months**, not 90 days — this is an academic deployment where
   term-over-term comparison is the natural analysis, and G0.4 will almost certainly show the storage
   cost of being generous is trivial. Revisit only if G0.4 contradicts it.
5. **Percentiles:** exact via `percentile_agg` if G0.3 says the toolkit is present; otherwise
   fixed-bucket counts in the aggregate — ugly, exact, dependency-free.
6. Lifetime totals survive retention because `entity_stats`/`model_stats` are cumulative and written
   synchronously. **Say so in the UI** next to any truncated chart.
7. Policies live in a dialect-guarded Alembic migration, not in hot-reloaded config. DDL from a config
   watcher is a bad idea.

**Tests** (all `@pytest.mark.postgres`): per-entity `/usage` queries return identical results before
and after being rewritten onto the new aggregate; a **retention-drop simulation** — seed rows older
than the window, drop the chunks, assert per-user charts still render from the aggregate and
`entity_stats` lifetime totals are unchanged; compression round-trip leaves query results identical;
the 1-minute aggregate is within one bucket of the raw table for a synthetic burst.

**[GATE] Phase 8 exit** — retention is enabled **only after** a dry run on a copy of production
demonstrates no per-user chart changes. If it changes anything, the phase stops.

---

## 13. Phase 9 — admission control (separate decision)

Listed for completeness and explicitly **not** part of the observability work.

A bounded per-model in-flight cap near the backend's `max_num_seqs`, a bounded queue with a deadline,
and a fast `429 + Retry-After` beyond it. It converts the invisible unbounded Q1 queue into a
bounded, measured, explainable one, and a student told "the model is busy, try again in 30 s" is far
better served than one who watches a spinner for ten minutes and is then cut by the 600 s gateway.

**[DECISION] This is a policy change, not a metrics change** — it moves the failure mode from
"everyone waits" to "some are rejected quickly", and needs the operators' agreement, not an
engineer's. It also is the second thing in this plan that genuinely requires Redis at M replicas (a
distributed semaphore). Do it after Phases 1–7 have produced the data to size the cap, or not at all.

---

## 13a. Burst pathologies this plan did not originally address

Surfaced by adversarial review. Each is a real load pathology that measurement alone will not fix;
each is listed with the phase that should at minimum *measure* it.

1. **The synchronized-completion write spike.** 300 streams that started together finish together,
   and each completion fires `update_stats` — five statements plus `subtract_coins` plus, on the API
   path, the `APIKey` update, in one transaction. That is well over a thousand statements arriving at
   Postgres within a few seconds, against a pool that Phase 4 has just stopped `/metrics` from
   competing for. **The plan decoupled the scrape from the DB and left the completion path
   untouched.** *Action:* measure it first — Phase 3's per-request columns plus the pool-wait
   histogram will show whether commit latency is a real component of `duration` under burst. Only if
   it is, consider batching the rollup updates or moving billing to a short queue — and treat that as
   a billing-correctness change with its own review, not a metrics change.
2. **No connection reuse upstream.** Every call site builds a fresh `openai.OpenAI(...)` inside a
   `with`, so a new httpx pool is created and destroyed per request: TCP + TLS on every single call.
   300 simultaneous handshakes to one backend is measurable latency and CPU at exactly the wrong
   moment. *Action:* Phase 3 makes `connect` visible (it is the gap between `preflight` end and
   `ttft`). A long-lived per-endpoint client is the obvious win **if the data justifies it**, and it
   carries its own concurrency and credential-rotation implications — measure, then decide.
3. **`pool_tracker`'s cost scales with concurrency, not duration.** It captures a 25-frame stack on
   *every* checkout, several times per request. Its per-request cost is negligible against an LLM
   call; its cost during a 300-request burst is the one always-on instrumentation that grows with the
   thing being diagnosed. **[DECISION] Do not remove it** — it caught a production leak nothing else
   could, twice. *Action:* measure its share under the Phase 2 load test, and add a sampling switch
   only if the measurement justifies it. **[OPEN]** for the team.
4. **Gateway-budget exhaustion is invisible.** Handled as Phase 2 item 6 above.

---

## 14. Standing invariants, enforced by test

The pattern already exists in this repo — `tests/unit/test_no_stream_with_context.py` and
`tests/unit/test_wsgi_disconnect_body.py` are static/behavioural guards against regressions that bit
production. These join them.

| Invariant | Enforced by |
|---|---|
| **No DB write per token, chunk, or interval of a stream.** Accumulate in the generator frame; write once | `tests/unit/test_llm_functions.py` — statement counter across a 500-chunk stream |
| **No `Histogram.observe` per token.** `observe` takes a lock; 50 tok/s × 300 streams is 15 000 lock acquisitions/s for a number nobody reads | `tests/unit/test_metrics_middleware.py` — patched `observe` counter |
| **No Redis call per token.** ≤ 2 per request | `tests/unit/test_live_state.py` — counting fake client |
| **No Redis on the critical path.** Every failure mode of Redis leaves proxying unaffected | `tests/unit/test_live_state.py` — raising and sleeping fake clients |
| **Every `Gauge` declares `multiprocess_mode`** | static scan over `lumen/`, same shape as `tests/unit/test_no_stream_with_context.py` |
| **Every live counter returns to zero** after any exit path | `tests/unit/test_live_state.py`, `tests/unit/test_wsgi_queue_metrics.py`, parametrised over normal / raise / disconnect / stall / timeout |
| **`/metrics` and `/admin/api/status` execute zero SQL** | `tests/routes/test_metrics_routes.py`, `tests/routes/test_admin_routes.py` — `before_cursor_execute` listener |
| **No DB session and no Flask context spans a `yield`** (existing CLAUDE.md rule; the snapshot refresher and the backend scraper are both new generators-adjacent daemons) | `tests/unit/test_no_stream_with_context.py` (existing), `tests/unit/test_db_teardown.py` extended to the new threads |
| **`time.monotonic()` for durations, wall clock only for stored timestamps** | review checklist; the existing LLM path uses `time.time()` throughout, which is NTP-step-sensitive — a wart worth not replicating |
| **No user identity in any Prometheus label** | static scan of label names against a denylist (`entity`, `user`, `email`, `api_key`) |

---

## 15. Corrections to the proposal

Verified against `196f642`; the proposal was written against an earlier tree.

1. **§5.3 is stale.** The claim that `lumen_http_request_duration_seconds` stops its clock when
   `wsgi_app()` returns, with a 10 s top bucket, **has already been fixed on this branch.**
   `lumen/blueprints/metrics/middleware.py:27-36` now runs buckets to 300 s with a comment explaining exactly this, and the
   observation happens in `_ContextCheckingBody.close()` — the change the proposal recommends. Remove
   this item from Phase 0; it is done.
2. **§8's "Redis is already an installed dependency" understates and overstates at once.** The Python
   package is installed unconditionally (`pyproject.toml`, `flask-limiter[redis]`), but the chart
   ships it **disabled**, so in a default deployment Redis is not running at all. "Not a new
   dependency decision" is true only if Gate 0 finds it already deployed.
3. **§8's central premise is a chart default, not production config** — the point you raised. The
   conclusion happens to survive for the *storage* question (§1.1 strengthens it) but not for the
   *live gauge* question, which is why §2 introduces the seam rather than a dict.
4. **§2.1's "multi-process aggregation is wired but currently moot" is too generous.** It is wired at
   the read side only. Without the volume mount, the startup wipe and explicit `multiprocess_mode` on
   every new gauge, turning on `--workers` would produce **wrong numbers**, not merely per-process
   ones. That is Phase 1.
5. **The coin refiller is multi-process safe**, contrary to the implication in §8's open question.
   `lumen/services/token_refill.py:101-113` is a compare-and-set on `last_refill_at`; a second process's pass is a
   no-op. The health checker's N× probe load is the real multi-process wart, and it is benign.
6. **Timescale is not optional.** §9's framing treats the hypertable as a Postgres-only enhancement;
   `CREATE EXTENSION ... CASCADE` in the migration makes it a hard boot requirement. This is worth
   stating in `docs/architecture.md` — an operator pointing Lumen at a plain Postgres today gets a
   failed migration, not a degraded feature.

---

## 16. Adversarial review — what changed

This plan was reviewed by GLM 5.2 (via `opencode`, run against this repo with its own independent
code dive) under a brief that told it a review finding nothing is a failed review, and instructed it
not to trust this document's `[FACT]` citations. It read the code and checked them. Findings, with
this plan's disposition:

| # | Finding | Disposition |
|---|---|---|
| **C1** | `time − duration − queue_wait` does not equal T0. `duration` starts *after* preflight (`lumen/services/llm.py:752` vs the context block at `:725-750`) and `time` is stamped *after* billing (`:848` → `:554`), so the derived start is `T0 + preflight + billing_delay` — and the error peaks during bursts, when pool contention makes preflight largest | **Accepted; this was a real bug in the plan.** Phase 3 now stores absolute `started_at` plus `preflight` instead of deriving. §7 items 8–9 |
| **M1** | `mark_process_dead` never runs under SIGKILL, so a crashed worker's `livesum` gauge file poisons the fleet number for the pod's remaining life; the dir wipe is per-pod, not per-worker | **Accepted.** Phase 1 item 3 now specifies a dead-PID reaping pass at worker start and on scrape |
| **M2** | Capturing T1 in the Prometheus middleware would NULL `queue_wait` whenever Prometheus is disabled — which is the chart default | **Accepted; a design error.** Phase 2 item 1 now requires unconditional capture; only histograms live in `lumen/blueprints/metrics/middleware.py` |
| **M3** | The seeded-data "headline query" test proves the SQL, never the data, and would have passed with C1 shipped | **Accepted.** New `tests/integration/test_started_at_accuracy.py` asserts `started_at` against an independently recorded T0 |
| **M4** | `lumen/services/health.py:27`'s executor is per-process, so N processes means N×8 probe threads at the backends every 60 s — the plan's "accept and document" underweighted this | **Accepted.** Phase 1 item 7 now elects a single runner via `fcntl.flock` on a pod-scoped file |
| **m1** | `LiveState` is an interface with one implementation if Gate 0 finds 1×1 — the speculative generality CLAUDE.md §2 forbids | **Accepted.** §2 now ships a plain class at 1×1 and adds the abstraction with the second implementation |
| **m2** | The `url_rule` cardinality fix is not a one-liner; `request.url_rule` is unset when the middleware captures the path | **Accepted, remedy corrected.** The review proposed reading `url_rule` after `wsgi_app` returns; verified against the installed Flask that `wsgi_app` runs `ctx.pop()` in its `finally` **before** returning, so the context is gone there too. Phase 1 item 5 now stashes the rule into `environ` from inside the request |
| **m3** | Column defaults unspecified; `send_blocked` ambiguous on non-streaming paths | **Accepted.** Defaults now stated explicitly, including why `outcome` gets none |
| **m4** | Counter files from dead PIDs are summed *correctly*; not saying so invites a wrong "fix" | **Accepted.** Phase 1 item 4 now states the asymmetry |
| **Idea 3** | Synchronized-completion write spike — the plan decoupled the scrape and ignored the completion path | **Accepted as §13a.1**, measure-first |
| **Idea 4** | No upstream connection reuse; 300 simultaneous TLS handshakes under burst | **Accepted as §13a.2**, measure-then-decide |
| **Idea 5** | Gateway-budget exhaustion recorded as an ordinary disconnect | **Accepted as Phase 2 item 6** — shed before the gateway, distinct `timeout` outcome |
| **Idea 6** | `pool_tracker`'s stack capture is the one instrumentation whose cost scales with concurrency | **Accepted as §13a.3**, left `[OPEN]` — it has twice caught leaks nothing else could |
| **Idea 7** | If Gate 0.5 finds no Prometheus stack, Phases 1–6 have no consumer and `/admin/status` should ship right after Phase 4 | **Accepted.** §11 now opens with that conditional |

Two things the review confirmed rather than challenged, worth recording because they were the
load-bearing claims: **Timescale is mandatory** (`i9j0k1l2m3n4:38` + `entrypoint.sh:8`), and
**Phase 8's ordering constraint** — entity-dimensioned aggregate strictly before retention — which it
independently called the most important risk decision in the document.

The review also caught that both documents cite the LLM module as `lumen/services/llm.py` when it is
`lumen/services/llm.py`. Line numbers are correct; the paths are not.

### Round 2

The revised plan went back to the same reviewer under a brief warning that agreeableness is the
round-2 failure mode. It re-verified against the installed Flask and the four LLM code paths.

**On the one finding this plan overrode:** round 1 claimed `request.url_rule` is readable after
`wsgi_app` returns; this plan rejected that. Round 2 read Flask 3.1.3 and **confirmed the rejection** —
`ctx.pop(error)` runs in `wsgi_app`'s `finally` before it returns — and confirmed the `environ`
replacement works, while pointing out `teardown_request` is strictly safer than `after_request`.
Adopted.

| # | Finding | Disposition |
|---|---|---|
| **A1** | `preflight = T2 − T1` subtracts a wall-clock T2 (`lumen/services/llm.py:752`) from a monotonic T1 — three different clocks across `queue_wait`/`preflight`/`ttft` | **Accepted.** Converting `t0` to `time.monotonic()` on all four paths is now a required Phase 3 change, not a §14 aspiration. Contract (a) |
| **A2** | Nothing said how `started_at`/`queue_wait` reach `update_stats`, and two of its five call sites are context-free generators that must not touch `request` | **Accepted.** Contract (c) specifies the `client_disconnect_event()` capture-into-closure pattern the codebase already documents |
| **A3** | `send_blocked` has no measurement mechanism — `send()` accumulates nothing, so the column ships permanently 0 | **Accepted.** Contract (e) specifies the accumulation, or cuts the column rather than shipping a zero that reads as "no backpressure ever" |
| **A4** | A blocking flock is worse than the problem: `lumen/services/health.py:81` commits inside the pass and can block on an exhausted pool, stalling all probing during a burst | **Accepted.** `LOCK_NB` + pass deadline + heartbeat |
| **A5** | `record_stream_abort(started_at=...)` (`lumen/services/llm.py:654`, called at `:772`) already means T2 | **Accepted.** Rename to `stream_t0` in the same change |
| **A6** | The `started_at` accuracy test needs an ASGI harness the plan never specified | **Accepted, and corrected** — `tests/integration/test_disconnect.py:205-226` already runs real uvicorn against `asgi.app`; reuse it |
| **B1** | Gateway shedding catches only budget-already-spent-at-T2; a stream that exhausts the budget mid-generation is still recorded as `disconnect` — the conflation the item claimed to fix | **Accepted.** Phase 2 item 6 now adds a mid-stream deadline check at the existing per-chunk poll, and says plainly what the admission check alone does not cover |
| **B2** | Budget formula omitted `preflight` | **Accepted.** Now `gateway.timeout − (T2 − T0)` |
| **B3** | Dead-PID reaping is fooled by PID reuse | **Accepted as a documented limitation** |
| **B4** | §13a.1 referenced a pool-wait histogram no phase defines | **Accepted.** Now Phase 2 item 7 |
| **C1** | `outcome` enum lacks `billing_error`, which `lumen/services/llm.py:845`'s `phase` already distinguishes and the abort counter already emits | **Accepted** |
| **C2/C3** | `preflight` is not uniform: audio's is dominated by `upload.read()` multipart parsing; the chat stream has two preflights spanned by one column | **Accepted** — documented per-path rather than pretending uniformity |
| **C4** | `TIMESTAMPTZ` + the mandated `utcnow()` (naive) is a silent timezone bug | **Accepted.** Contract (b) requires `datetime.now(timezone.utc)`, matching what `request_logs.time` already does |

**Cuts proposed, and the response.** Round 2 suggested cutting §3 (the Redis analysis), §16, §13a.3
and Phase 9. **Declined for §3** — it is the direct answer to the question that commissioned this
work, and compressing it would lose the reasoning rather than the words. **Declined for §13a.3** —
an `[OPEN]` with a named measurement is how a known cost stays known. §16 and Phase 9 are one table
and one paragraph respectively; they stay unless the team wants a leaner document.

**Buildability, per round 2:** Phase 1 was executable with minor questions; Phases 2–3 had four
places an engineer would have to stop and design (A1, A2, A3, A6). All four are now decided in the
implementation contract above. That was the point of the round.

### Round 3

Two reviews, run against material nobody had checked: (3a) the round-2 fixes, and (3b) Phases 4–8,
which no round had assessed. Both found more than rounds 1 and 2 did. This is the third consecutive
round in which **the corrections were the weakest text in the document** — the pattern is now
established well enough to treat as a rule (see the lessons doc, §1.1).

#### 3a — the round-2 fixes. Two criticals.

| # | Finding | Disposition |
|---|---|---|
| **C1** | Contract (a) scoped the monotonic conversion to "the four paths' own `duration` lines" and missed that `t0` **escapes its function**: `record_stream_abort` computes `duration = time.time() - started_at` at `lumen/services/llm.py:686` from the caller's `t0`. Converting without it writes **`duration ≈ 1.76e9`** — ~55 years — on every aborted row, silently. `t_first` (`:819`) is a second such site and is user-visible in the chat UI | **Accepted; verified myself.** Contract (a) now enumerates all six sites and mandates a grep-shaped static test |
| **C2** | Four of the six `outcome` values are unwritable. `billing_error` especially: `update_stats` only `flush()`es, the caller `commit()`s, so the failing commit rolls back the very row that would record the failure. The round-2 argument for including it was exactly backwards | **Accepted.** Enum cut to `ok` and `disconnect`; any further value must arrive with the code that writes it |
| **M3** | The 429 in §6 item 6 is impossible where specified — on both streaming paths, `start_response` has already committed **200** before the generator runs, leaving only an in-band SSE error the OpenAI SDK will not retry | **Accepted.** Admission check moves into the views, before `Response(...)`, using T1 |
| **M4** | The budget it compares against does not exist in the app: `gateway.timeout` is Helm-only, consumed by `httproute.yaml` under `gateway.enabled` (default false), never rendered into `config.yaml` | **Accepted.** New hot-loaded `api.request_budget_seconds`, plus a safety margin for the inter-chunk check |
| **M5** | Contract (e) records **zero** for the stalled client (the timeout branch raises without accumulating), and the view cannot read the value because streaming billing happens in the generator | **Accepted.** Accumulate in a `finally`; publish a mutable holder in `environ` |
| **M6** | Contract (c) omitted the fifth `update_stats` call site (`record_aborted_request` via `record_stream_abort`) — so aborted rows, the ones the headline query needs most, would carry NULLs. Its `None` fallback also made the Phase 3 "all four paths populate" test unsatisfiable under `test_client` | **Accepted.** Both signatures threaded; the assertion moves to the real-uvicorn harness |
| **M7** | `mark_process_dead` does an unguarded `os.remove`; concurrent reaps 500 the scrape — clustering right after an OOM kill | **Accepted.** Wrapped, and ordered before `MultiProcessCollector` |
| **M8** | The cardinality fix bounds `path_template` and leaves `method` unbounded, so the Phase 1 gate would pass with the bug open | **Accepted.** Allow-list methods; the gate now scans junk methods too |
| **M9** | The flock's "pass deadline" cannot be implemented — nothing in the pass is preemptible, so releasing the lock admits a concurrent second pass. Plus: `"w"` truncates before `flock`, and the proposed emptyDir path does not exist at defaults | **Accepted.** Deadline dropped for mtime-staleness takeover; open mode and a fixed container path specified |
| **m10–m13** | `record_stream_abort` has two callers (renaming one leaves a `TypeError` inside `_abort()` → `RuntimeError: generator ignored GeneratorExit` → abort never billed); PID reuse inherits the dead worker's mmap rather than merely leaving a stale file; `send_blocked = 0.0` on non-streaming paths is justified by the wrong reason; chat `preflight` also spans a `send()` of the response-start | **All accepted** |

#### 3b — Phases 4–8 buildability. The structural finding matters more than any single item.

**Phases 4–8 are not buildable as written.** §7 has an implementation contract because round 2 forced
one into existence; Phases 4–8 have none, which is why they *read* as buildable. Fourteen findings,
of which these change the design rather than the prose:

- **F1 (critical):** Phase 4 says to model the snapshot refresher on `lumen/services/health.py` — but
  Phase 1 item 7 has just turned that file into a **single-runner election**. Copying it would leave
  N−1 workers serving an empty snapshot; Prometheus reads absent-then-present as a counter **reset**,
  producing sawtooth garbage in the multi-process deployment Phase 1 exists to enable. The refresher
  must run in **every** process and must never be flock-elected: election is for work whose result
  lands in the DB, not for in-memory snapshots. Also unaddressed: per-process refresh means Phase 4
  can *increase* total DB load rather than reduce it.
- **F2 (critical):** Phase 5's Redis structure is wrong. **Redis sets have no per-member TTL**, and
  `SREM` on a user's first of three concurrent requests removes them while two are still in flight —
  under-reporting exactly the double-submitting student the burst scenario is full of. `SCARD` is
  also offered as both unique-users and in-flight, which are different numbers. Needs a sorted set
  keyed `{entity_id}:{request_id}` with expiry as score; `LocalLiveState` needs the same multiplicity
  fix (a counter, not a set).
- **F3:** `admit`/`release` has no contract — call sites, disconnect-path release, and whether
  "waiting" ends at TTFT or at completion are all undecided. The last changes the headline number by
  ~10× during a long generation, and it drives Phase 7's "N ahead of you" and Phase 9's cap sizing.
- **F4/F5:** Phase 8's ordering omits the step that actually protects per-user history — **rewriting
  the `/usage` per-entity queries onto the new aggregate**. Creating the aggregate protects nothing
  on its own, so following §12's numbered list literally still truncates every user's "All Time"
  chart. And the entity aggregate itself is unspecified: bucket width (daily collapses the heatmap's
  `EXTRACT(HOUR …)` to hour 0), and a full-history backfill that **cannot run inside an Alembic
  transaction**.
- **F7:** Phase 7 consumes Phase 8's 1-minute aggregate and its percentile decision, while the G0.5
  clause authorises moving Phase 7 *earlier*. Split it: 7a live tiles (may jump the queue, zero SQL),
  7b historical charts (strictly after §12.2).
- **F9:** Phase 6's "already detects the backend type" is half true — only SGLang is ever tagged;
  vLLM is the fall-through, indistinguishable from OpenAI, Azure, or any OpenAI-compatible proxy.
  Guessing "not-sglang ⇒ vllm" would send scrapes, possibly carrying the endpoint's API key, to
  arbitrary third-party hosts. And vLLM exposes no capacity gauge, so the denominator must be
  operator-configured.
- **F6, F8, F10–F14:** the refresher's real leak mode is a context spanning `sleep` (CLAUDE.md's rule
  says `yield`, so it reads as silent — worth generalising the rule); nobody owns the snapshot's
  schema while three phases add to it; compression is a one-way door for further `request_logs`
  columns; and several smaller unmade decisions.

**[DECISION] Phases 4–8 do not start until each has an implementation contract in the shape of §7's.**
Phase 4's must at minimum settle: the refresher runs per-process and unelected; the snapshot's schema
and each field's query cost; `admit`/`release` call sites and the meaning of "waiting"; one
`app_context()` per pass, exited before the sleep.
