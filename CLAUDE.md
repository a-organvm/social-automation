# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

`kerygma_social` — the POSSE distribution and social platform automation package for ORGAN-VII (Kerygma). Provides platform clients for Mastodon, Discord, Bluesky, and Ghost, a `PosseDistributor` orchestrator, and a resilience stack (retry, circuit breaker, rate limiter).

## Package Structure

Source is in `kerygma_social/`, installed as the `kerygma_social` package:

### Platform Clients
| Module | Class | Protocol |
|--------|-------|----------|
| `mastodon.py` | `MastodonClient` | REST API via `urllib`. `MastodonConfig` dataclass. `Toot` dataclass for posts. `format_for_mastodon()` helper. |
| `discord.py` | `DiscordWebhook` | Webhook POST. `DiscordEmbed` dataclass for rich embeds. |
| `bluesky.py` | `BlueskyClient` | AT Protocol. `BlueskyConfig`/`BlueskyPost` dataclasses. Session-based auth. |
| `ghost.py` | `GhostClient` | Admin API with JWT auth (`HS256`, `{id}:{secret}` key format). `GhostConfig`/`GhostPost` dataclasses. Optional newsletter targeting. |

### Orchestration & Resilience
| Module | Purpose |
|--------|---------|
| `posse.py` | `PosseDistributor` — central dispatcher. `create_post()` → `syndicate()` flow. `_with_resilience()` wraps calls: rate limiter (outermost) → circuit breaker → retry (innermost). Deduplicates via `DeliveryLog`. Enums: `Platform`, `SyndicationStatus`. Dataclasses: `ContentPost`, `SyndicationRecord`. |
| `circuit_breaker.py` | Three-state machine: CLOSED → OPEN → HALF_OPEN. `CircuitBreakerConfig` (failure_threshold=5, reset_timeout=60s). Raises `CircuitOpenError` when open. |
| `retry.py` | Exponential backoff retry. `RetryConfig` dataclass. |
| `rate_limiter.py` | Token bucket rate limiter. `RateLimiterConfig` dataclass. `acquire(block=True)` blocks until token available. |
| `delivery_log.py` | `DeliveryLog` — persistent JSON log of dispatch attempts. `has_been_delivered(post_id, platform)` for deduplication. `DeliveryRecord` dataclass. |
| `rss_poller.py` | `RssPoller` — polls RSS/Atom feeds via stdlib `xml.etree` and `urllib`. Tracks seen entry IDs in JSON. `parse_feed()` handles both Atom and RSS 2.0. |

### Configuration
| Module | Purpose |
|--------|---------|
| `config.py` | `load_config(path)` → `SocialConfig` dataclass. YAML file + env var overrides (prefix `KERYGMA_`). All platform credentials, `live_mode`, `delivery_log_path`, `rss_feed_url`. |
| `cli.py` | CLI entry point (`social-dispatch`). |

## Development Commands

```bash
# Install (from superproject root or this directory)
pip install -e .[dev]

# Tests
pytest tests/ -v
pytest tests/test_posse.py::TestPosseDistributor::test_syndicate_mastodon -v

# Lint
ruff check kerygma_social/
```

## Key Design Details

- **All clients accept `live=False`** (default). In dry-run mode, API calls return mock responses. Never commit config with `live_mode: true`.
- **Resilience ordering matters**: rate limiter prevents burst → circuit breaker fails fast if service is down → retry handles transient errors. `CircuitOpenError` propagates immediately (not retried).
- **RSS poller is stdlib-only** — uses `urllib.request` and `xml.etree.ElementTree`. No `feedparser` dependency at the package level (feedparser is only needed by the pipeline orchestrator).
- **Delivery log uses atomic writes** — writes to `.tmp` then `os.replace()`.
- **Runtime dependency**: only `pyyaml>=6.0` (for config loading).

## Test Structure

Tests in `tests/` with `fixtures/` directory for test config files:
- `test_mastodon.py`, `test_discord.py`, `test_bluesky.py`, `test_ghost.py` — per-client formatting and dispatch
- `test_posse.py` — PosseDistributor syndication, deduplication, resilience
- `test_circuit_breaker.py` — state transitions, threshold, recovery
- `test_retry.py` — backoff behavior, max retries
- `test_rate_limiter.py` — token bucket, blocking acquire
- `test_delivery_log.py` — persistence, dedup checks
- `test_rss_poller.py` — Atom/RSS parsing, seen tracking
- `test_config.py` — YAML loading, env var overrides

<!-- ORGANVM:AUTO:START -->
## System Context (auto-generated — do not edit)

**Organ:** ORGAN-VII (Marketing) | **Tier:** standard | **Status:** GRADUATED
**Org:** `organvm-vii-kerygma` | **Repo:** `social-automation`

### Edges
- **Produces** → `ORGAN-IV`: delivery_log

### Siblings in Marketing
`announcement-templates`, `distribution-strategy`, `.github`, `kerygma-pipeline`, `kerygma-profiles`

### Governance
- *Standard ORGANVM governance applies*

*Last synced: 2026-04-14T21:32:11Z*

## Active Handoff Protocol

If `.conductor/active-handoff.md` exists, **READ IT FIRST** before doing any work.
It contains constraints, locked files, conventions, and completed work from the
originating agent. You MUST honor all constraints listed there.

If the handoff says "CROSS-VERIFICATION REQUIRED", your self-assessment will
NOT be trusted. A different agent will verify your output against these constraints.

## Session Review Protocol

At the end of each session that produces or modifies files:
1. Run `organvm session review --latest` to get a session summary
2. Check for unimplemented plans: `organvm session plans --project .`
3. Export significant sessions: `organvm session export <id> --slug <slug>`
4. Run `organvm prompts distill --dry-run` to detect uncovered operational patterns

Transcripts are on-demand (never committed):
- `organvm session transcript <id>` — conversation summary
- `organvm session transcript <id> --unabridged` — full audit trail
- `organvm session prompts <id>` — human prompts only


## System Library

Plans: 269 indexed | Chains: 5 available | SOPs: 121 active
Discover: `organvm plans search <query>` | `organvm chains list` | `organvm sop lifecycle`
Library: `meta-organvm/praxis-perpetua/library/`


## Active Directives

| Scope | Phase | Name | Description |
|-------|-------|------|-------------|
| system | any | atomic-clock | The Atomic Clock |
| system | any | execution-sequence | Execution Sequence |
| system | any | multi-agent-dispatch | Multi-Agent Dispatch |
| system | any | session-handoff-avalanche | Session Handoff Avalanche |
| system | any | system-loops | System Loops |
| system | any | prompting-standards | Prompting Standards |
| system | any | research-standards-bibliography | APPENDIX: Research Standards Bibliography |
| system | any | phase-closing-and-forward-plan | METADOC: Phase-Closing Commemoration & Forward Attack Plan |
| system | any | research-standards | METADOC: Architectural Typology & Research Standards |
| system | any | sop-ecosystem | METADOC: SOP Ecosystem — Taxonomy, Inventory & Coverage |
| system | any | autonomous-content-syndication | SOP: Autonomous Content Syndication (The Broadcast Protocol) |
| system | any | autopoietic-systems-diagnostics | SOP: Autopoietic Systems Diagnostics (The Mirror of Eternity) |
| system | any | background-task-resilience | background-task-resilience |
| system | any | cicd-resilience-and-recovery | SOP: CI/CD Pipeline Resilience & Recovery |
| system | any | community-event-facilitation | SOP: Community Event Facilitation (The Dialectic Crucible) |
| system | any | context-window-conservation | context-window-conservation |
| system | any | conversation-to-content-pipeline | SOP — Conversation-to-Content Pipeline |
| system | any | cross-agent-handoff | SOP: Cross-Agent Session Handoff |
| system | any | cross-channel-publishing-metrics | SOP: Cross-Channel Publishing Metrics (The Echo Protocol) |
| system | any | data-migration-and-backup | SOP: Data Migration and Backup Protocol (The Memory Vault) |
| system | any | document-audit-feature-extraction | SOP: Document Audit & Feature Extraction |
| system | any | dynamic-lens-assembly | SOP: Dynamic Lens Assembly |
| system | any | essay-publishing-and-distribution | SOP: Essay Publishing & Distribution |
| system | any | formal-methods-applied-protocols | SOP: Formal Methods Applied Protocols |
| system | any | formal-methods-master-taxonomy | SOP: Formal Methods Master Taxonomy (The Blueprint of Proof) |
| system | any | formal-methods-tla-pluscal | SOP: Formal Methods — TLA+ and PlusCal Verification (The Blueprint Verifier) |
| system | any | generative-art-deployment | SOP: Generative Art Deployment (The Gallery Protocol) |
| system | any | market-gap-analysis | SOP: Full-Breath Market-Gap Analysis & Defensive Parrying |
| system | any | mcp-server-fleet-management | SOP: MCP Server Fleet Management (The Server Protocol) |
| system | any | multi-agent-swarm-orchestration | SOP: Multi-Agent Swarm Orchestration (The Polymorphic Swarm) |
| system | any | network-testament-protocol | SOP: Network Testament Protocol (The Mirror Protocol) |
| system | any | open-source-licensing-and-ip | SOP: Open Source Licensing and IP (The Commons Protocol) |
| system | any | performance-interface-design | SOP: Performance Interface Design (The Stage Protocol) |
| system | any | pitch-deck-rollout | SOP: Pitch Deck Generation & Rollout |
| system | any | polymorphic-agent-testing | SOP: Polymorphic Agent Testing (The Adversarial Protocol) |
| system | any | promotion-and-state-transitions | SOP: Promotion & State Transitions |
| system | any | recursive-study-feedback | SOP: Recursive Study & Feedback Loop (The Ouroboros) |
| system | any | repo-onboarding-and-habitat-creation | SOP: Repo Onboarding & Habitat Creation |
| system | any | research-to-implementation-pipeline | SOP: Research-to-Implementation Pipeline (The Gold Path) |
| system | any | security-and-accessibility-audit | SOP: Security & Accessibility Audit |
| system | any | session-self-critique | session-self-critique |
| system | any | smart-contract-audit-and-legal-wrap | SOP: Smart Contract Audit and Legal Wrap (The Ledger Protocol) |
| system | any | source-evaluation-and-bibliography | SOP: Source Evaluation & Annotated Bibliography (The Refinery) |
| system | any | stranger-test-protocol | SOP: Stranger Test Protocol |
| system | any | strategic-foresight-and-futures | SOP: Strategic Foresight & Futures (The Telescope) |
| system | any | styx-pipeline-traversal | SOP: Styx Pipeline Traversal (The 7-Organ Transmutation) |
| system | any | system-dashboard-telemetry | SOP: System Dashboard Telemetry (The Panopticon Protocol) |
| system | any | the-descent-protocol | the-descent-protocol |
| system | any | the-membrane-protocol | the-membrane-protocol |
| system | any | theoretical-concept-versioning | SOP: Theoretical Concept Versioning (The Epistemic Protocol) |
| system | any | theory-to-concrete-gate | theory-to-concrete-gate |
| system | any | typological-hermeneutic-analysis | SOP: Typological & Hermeneutic Analysis (The Archaeology) |

Linked skills: cicd-resilience-and-recovery, continuous-learning-agent, evaluation-to-growth, genesis-dna, multi-agent-workforce-planner, promotion-and-state-transitions, quality-gate-baseline-calibration, repo-onboarding-and-habitat-creation, structural-integrity-audit


**Prompting (Anthropic)**: context 200K tokens, format: XML tags, thinking: extended thinking (budget_tokens)


## Ecosystem Status

- **delivery**: 2/2 live, 0 planned
- **marketing**: 1/1 live, 0 planned
- **content**: 0/1 live, 0 planned

Run: `organvm ecosystem show social-automation` | `organvm ecosystem validate --organ VII`


## Entity Identity (Ontologia)

**UID:** `ent_repo_01KKKX3RVQ8FX747Z2EZ4FGMXJ` | **Matched by:** primary_name

Resolve: `organvm ontologia resolve social-automation` | History: `organvm ontologia history ent_repo_01KKKX3RVQ8FX747Z2EZ4FGMXJ`


## Live System Variables (Ontologia)

| Variable | Value | Scope | Updated |
|----------|-------|-------|---------|
| `active_repos` | 89 | global | 2026-04-14 |
| `archived_repos` | 54 | global | 2026-04-14 |
| `ci_workflows` | 107 | global | 2026-04-14 |
| `code_files` | 0 | global | 2026-04-14 |
| `dependency_edges` | 60 | global | 2026-04-14 |
| `operational_organs` | 10 | global | 2026-04-14 |
| `published_essays` | 29 | global | 2026-04-14 |
| `repos_with_tests` | 0 | global | 2026-04-14 |
| `sprints_completed` | 33 | global | 2026-04-14 |
| `test_files` | 0 | global | 2026-04-14 |
| `total_organs` | 10 | global | 2026-04-14 |
| `total_repos` | 145 | global | 2026-04-14 |
| `total_words_formatted` | 0 | global | 2026-04-14 |
| `total_words_numeric` | 0 | global | 2026-04-14 |
| `total_words_short` | 0K+ | global | 2026-04-14 |

Metrics: 9 registered | Observations: 32128 recorded
Resolve: `organvm ontologia status` | Refresh: `organvm refresh`


## System Density (auto-generated)

AMMOI: 58% | Edges: 42 | Tensions: 33 | Clusters: 5 | Adv: 23 | Events(24h): 32336
Structure: 8 organs / 145 repos / 1654 components (depth 17) | Inference: 98% | Organs: META-ORGANVM:65%, ORGAN-I:53%, ORGAN-II:48%, ORGAN-III:54% +5 more
Last pulse: 2026-04-14T21:31:36 | Δ24h: -1.0% | Δ7d: n/a


## Dialect Identity (Trivium)

**Dialect:** SIGNAL_PROPAGATION | **Classical Parallel:** Astronomy | **Translation Role:** The Broadcast — structure-preserving projection to external

Strongest translations: III (structural), VI (analogical), I (analogical)

Scan: `organvm trivium scan VII <OTHER>` | Matrix: `organvm trivium matrix` | Synthesize: `organvm trivium synthesize`


## Logos Documentation Layer

**Status:** MISSING | **Symmetry:** 0.0 (VACUUM)

Nature demands a documentation counterpart. This formation maintains its narrative record in `docs/logos/`.

### The Tetradic Counterpart
- **[Telos (Idealized Form)](../docs/logos/telos.md)** — The dream and theoretical grounding.
- **[Pragma (Concrete State)](../docs/logos/pragma.md)** — The honest account of what exists.
- **[Praxis (Remediation Plan)](../docs/logos/praxis.md)** — The attack vectors for evolution.
- **[Receptio (Reception)](../docs/logos/receptio.md)** — The account of the constructed polis.

### Alchemical I/O
- **[Source & Transmutation](../docs/logos/alchemical-io.md)** — Narrative of inputs, process, and returns.



*Compliance: Formation is currently void.*

<!-- ORGANVM:AUTO:END -->











## ⚡ Conductor OS Integration
This repository is a managed component of the ORGANVM meta-workspace.
- **Orchestration:** Use `conductor patch` for system status and work queue.
- **Lifecycle:** Follow the `FRAME -> SHAPE -> BUILD -> PROVE` workflow.
- **Governance:** Promotions are managed via `conductor wip promote`.
- **Intelligence:** Conductor MCP tools are available for routing and mission synthesis.