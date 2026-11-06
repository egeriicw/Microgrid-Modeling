# Product Requirements Document — Community Energy Profiler (working title)

**Version:** 2.0 DRAFT for owner review · **Date:** 2026-11-03
**Owner:** Bill Eger · **Supersedes:** [`archive/PRD-v1-snlg-platform.md`](./archive/PRD-v1-snlg-platform.md) (kept as a historical artifact)
**Sources:** owner interview (2026-11-02/03), [`codebase-review.md`](./codebase-review.md), full git history.
**Companions:** [`architecture-and-stack.md`](./architecture-and-stack.md), [`sprint-plan.md`](./sprint-plan.md), [`agent-team.md`](./agent-team.md).

> Status words used throughout: **MUST** = required for the 30-day MVP. **SHOULD** = target for the MVP, may slip.
> **LATER** = explicitly not in the MVP, but the design must not block it. Items marked **[verify]** are facts about
> outside tools or datasets that an agent must confirm before relying on them.

---

## 1. Summary

A web application that estimates the **hourly and 15-minute electricity load of a community** — a hypothetical
neighborhood defined by a building mix, a set of real mapped buildings, or a campus — **with quantified uncertainty**,
before any meters exist. It samples real buildings from NREL **ResStock** (residential) and **ComStock** (commercial)
to build many plausible neighborhoods, repeats until the average load **converges**, and presents the result as
interactive charts and downloadable data. A **calibration module** compares the synthetic result with real or benchmark
data so users (and the public) can see how much to trust it.

Origin: the **DC Transactive Microgrid** project, in support of a community-microgrid RFP response that was won.
First geography: **Washington, DC** (then Northern Virginia). The owner intends it to become a reusable and saleable
tool for microgrid analysis, RFPs, campus aggregation, and microgrid **policy** analysis, and to publish it publicly.

## 2. Problem

Community-microgrid planners (battery and solar sizing, PJM energy/capacity/ancillary revenue, critical-load resilience,
feeder impact) need a defensible load profile for a neighborhood that has no smart-meter history. Today that means
hand-built spreadsheets or notebooks, a single deterministic profile that hides diversity and peak risk, and no way for a
non-programmer to configure, run, inspect or trust the result. Existing tools (e.g. NREL REopt/SAM/dGen, HOMER, City
Energy Analyst **[verify]**) mostly assume a load profile is already known.

## 3. Goals and non-goals

### 3.1 Goals
- **G1** Make the load-forecasting core usable by a non-programmer through a web app (this is the success-or-failure core).
- **G2** Make results **trustworthy and honest**: convergence evidence, uncertainty bands, a calibration/validation
  module, benchmark checks, and a visible accuracy caveat.
- **G3** Be **statistically sound and replayable**: sampling is genuinely random (Monte Carlo) so ensembles represent plausible real neighborhoods; every run records its seeds so it can be replayed exactly (e.g. for calibration comparisons); and converged results must be statistically equivalent across different seeds.
- **G4** Be **extensible** without refactoring: new building types, new regions, weather normalization, DER, network,
  metering, policy constraints.
- **G5** Be **maintainable by a small human + agent team**: documented, test-driven, versioned, modular.
- **G6** Ship a **public, constrained demo** and (nice to have) a public repository by day 30.

### 3.2 Non-goals for the 30-day MVP
Power-flow physics, capacity checks, DER sizing and dispatch, revenue/economics, REopt/SAM file export, weather-driven
forecasting, EV/heat-pump scenarios, multi-region data, billing, full user administration,
arbitrary GIS uploads, open public simulation. These are **LATER** — see §9 for the hooks the MVP must leave.
Separately, heavy industrial/manufacturing buildings are **out of scope** (not merely later) unless ComStock defines them;
community-scale warehouses are **in scope** (FR-ENG-1).

## 4. Users and access tiers

| Tier | Who | Capabilities (MVP) |
|---|---|---|
| **Admin** | Owner | Everything: define any parameter, manage compositions and demo content, view calibration and diagnostics, configure the demo. |
| **Client** | Owner acting as first customer; later paying users | Full flexibility to define neighborhoods and parameters, run, view, download, upload calibration data. |
| **Demo** | Anonymous public visitor | Locked down: choose from **predefined compositions** only, view precomputed or capped runs, charts, and downloads. No arbitrary runs, no uploads. |

Business model (not built in MVP): subscription or pay-per-use. Persona stories: *Priya* (utility/DER planner),
*Marcus* (community microgrid developer writing an RFP), *the owner* (power user and publisher), *a campus energy
manager* (aggregating many meters), *a policy audience* (reading the public demo).

## 5. Core concepts (glossary)

- **Composition** — a recipe for a neighborhood: total buildings plus, per building type, either a fraction range or a
  fixed count. May mix both (hybrid of hypothetical and mapped).
- **Building type / category** — an entry in an extensible **registry** (e.g. `sf_detached`, `sf_attached`, `mf_small`,
  `mf_mid`, `mf_large`, `commercial_small_office`, `institutional_school`, `institutional_hospital`, `civic`, `commercial_warehouse`), each
  mapped to ResStock/ComStock filters. Heavy industrial/manufacturing is excluded; a community-scale warehouse is
  included **only if ComStock defines it** (agent confirms against the DC dataset **[verify]**).
- **Variation / realization** — one sampled neighborhood (one Monte-Carlo draw).
- **Ensemble** — the set of accepted variations in a run. **Converged** — see FR-ENG-6.
- **Load profile** — the aggregated electric demand time series (kW) at hourly and 15-minute resolution.
- **Meter** — a measurement point (placeholder in MVP; see FR-DATA-3).
- **Calibration / validation** — comparing synthetic output with measured or benchmark data.
- **Run** — one execution of an ensemble for a composition and settings; reproducible by stored seeds + version.

## 6. Functional requirements

Each requirement has an ID and **acceptance criteria (AC)** that an agent can turn into a failing test first.

### 6.1 Engine (existing code, hardened) — `ENG`
- **FR-ENG-1 (MUST) Building-type registry.** Types are data, not code. AC: adding a type means adding a registry entry
  and mapping rules; a test adds a fake type without changing engine code. MVP types: single-family detached/attached,
  multifamily (small/mid/large), small commercial (office etc.), institutional (school, hospital), civic, and community-scale warehouse (if defined in ComStock).
- **FR-ENG-2 (MUST) Composition modes.** Support fraction ranges, fixed counts, and a hybrid. AC: a composition with
  3 fixed mapped counts + 2 ranged categories yields integer counts summing exactly to `total_buildings`.
- **FR-ENG-3 (MUST) Config honesty.** Unknown or unsupported config keys raise an error listing them; none are silently
  ignored. AC: a config with `heatpump: 0.1` either applies it (tested) or is rejected with a clear message.
- **FR-ENG-4 (MUST) Seeding: random by default, replayable always.** A run uses fresh random seeds unless the user
  supplies fixed ones; seeds are always recorded with the run. Per-run RNG streams are derived with
  `numpy.random.SeedSequence.spawn`. AC: (a) same seeds + engine version + data manifest ⇒ byte-identical scalar
  metrics (determinism golden test); (b) 10,000 run indices produce distinct streams; (c) **seed-robustness test:** for a
  converged ensemble on fixture data, key metrics (annual energy, peak, P50 profile) from several different seeds agree
  within the ensemble's stated uncertainty; (d) the UI shows the seeds and offers "replay with same seeds".
- **FR-ENG-5 (MUST) Resolutions.** Output hourly and 15-minute profiles. AC: 15-minute totals equal source energy;
  hourly equals the sum of its four 15-minute values; units labeled kW/kWh explicitly.
- **FR-ENG-6 (MUST) Adaptive convergence.** Run up to `max_runs` (default 1,000), stopping early when convergence
  criteria hold. Criteria are a **methodology decision for the owner** (proposed default: relative change of the
  ensemble mean of annual energy, peak kW, and the mean hourly profile below ε over M consecutive checks, plus a
  standard-error target on the P50 profile). AC: criteria and thresholds are configurable and reported; a synthetic
  test ensemble with known variance converges in the expected number of runs.
- **FR-ENG-7 (MUST) Outputs.** Mean/P50, P10, P90 envelopes; residential vs commercial split (and per building type);
  peak, annual energy, load factor, coincidence and diversity factors (attached to results); load-duration curves;
  seasonal average daily profiles by load type; representative cases; composition summary.
- **FR-ENG-8 (MUST) Progress events.** The engine emits typed progress events (stage, run index, feasible count,
  convergence status, message). AC: a unit test collects events from a fixture run.
- **FR-ENG-9 (SHOULD) Perturbations.** End-use scaling must actually run when enabled (currently dormant), or be
  removed from the config surface. Requires owner approval (methodology).
- **FR-ENG-10 (MUST) Temperature.** Each result carries the temperature series used (from the ResStock/ComStock
  weather files) so charts can show load and temperature together. **[verify]** which weather file applies per dataset.

### 6.2 Data — `DATA`
- **FR-DATA-1 (MUST) DC ingestion.** A scripted, resumable path to provision DC ResStock/ComStock inputs into the
  `data/input` / `data/background` layout, with a manifest of files, sizes and checksums. Raw Parquet stays on disk, **not**
  in the database. AC: running the script on a clean machine reproduces the expected layout; disk use is reported.
- **FR-DATA-2 (MUST) Test fixtures.** A small committed fixture dataset (≤ ~5 buildings per type, checked in or
  generated) so tests and CI run without the full data. Agents in the cloud run only on fixtures.
- **FR-DATA-3 (MUST, placeholder) Meter hierarchy.** The data model has a `Meter` entity. In MVP each building has
  exactly one meter; meters roll up to **residential total, commercial total, and neighborhood total**. AC: results can
  be queried at building, class and total level. No metering behavior is built. Design must allow later: submeters under
  a combined meter, a campus with many meters, meters hosting solar/storage (virtual power plant / virtual net
  metering), and an aggregate meter at the distribution point of interconnection.
- **FR-DATA-4 (MUST) Storage.** PostgreSQL with PostGIS and TimescaleDB (Docker). Interval load results and uploaded
  calibration series in hypertables; geometry in PostGIS; configs, runs, users in plain tables. Alembic migrations.
- **FR-DATA-5 (LATER, placeholder) SEED coupling.** A defined adapter interface for DOE's **SEED** platform
  **[verify]** API, for storing or importing the final neighborhood and building records. MVP delivers only the interface
  and a decision record.
- **FR-DATA-6 (SHOULD) Building footprints, DC.** One footprint source for geometry (DC Open Data, OpenStreetMap,
  Microsoft or Overture), with attributes (use, stories, units, year built) joined from DC open government property
  data. An agent evaluates the options (coverage, attributes, license/attribution) and recommends one **for owner approval**.

### 6.3 Web application — `WEB`
- **FR-WEB-1 (MUST) Composition form.** Define total buildings and per-type fractions/fixed counts, seeds, `max_runs`,
  convergence settings; live validation; save, clone, and name compositions; import/export YAML.
- **FR-WEB-2 (MUST) Run with progress.** Start a run; see live stage text, progress bar, runs completed, feasible/rejected
  counts, and convergence status; cancel. Progress is shown for **every long operation** including data ingestion.
- **FR-WEB-3 (MUST) Charts (interactive).** Time-series trends for residential and commercial; load-duration curves;
  seasonal average daily profiles by load type; composition chart + table; P10/P50/P90 envelope; hover shows load (kW) and
  temperature at that timestamp; zoom/pan; switch hourly ↔ 15-minute.
- **FR-WEB-4 (MUST) Downloads.** CSV of hourly and 15-minute profiles (total, residential, commercial, per type, and
  percentiles) with a metadata header (run id, version, seeds, composition). Parquet is **LATER**.
- **FR-WEB-5 (MUST) Roles.** Admin, Client, Demo as in §4, enforced in the API (not only hidden in the UI).
- **FR-WEB-6 (MUST) Calibration screen.** See §6.4.
- **FR-WEB-7 (SHOULD) Minimal map viewer.** A DC map showing building footprints and a drawn/selected area producing
  a composition's fixed counts. **Demoted: not a failure condition if it slips.** Full GIS workspace is LATER.
- **FR-WEB-8 (MUST) Accuracy caveat.** Every results view shows: *methodology demonstration with benchmark checks — not
  validated for engineering or investment decisions*, plus the calibration status of the run.
- **FR-WEB-9 (SHOULD) Admin console.** Manage demo compositions and the demo account's allowed options.

### 6.4 Calibration and validation — `CAL`
- **FR-CAL-1 (MUST) Upload.** Client/Admin uploads a CSV (timestamp + kW, optional building class and name); the system
  validates format, timezone, gaps, duplicates, and units, and reports problems clearly.
- **FR-CAL-2 (MUST) Compare.** Overlay measured vs synthetic P10/P50/P90; compute and display NMBE, CV(RMSE), peak
  error, energy error, load-factor error, and the share of measured hours inside the P10–P90 band. Thresholds follow
  ASHRAE Guideline 14 hourly criteria as reference lines **[verify]**.
- **FR-CAL-3 (MUST) Benchmarks.** An admin-managed benchmark set (e.g. CBECS, RECS **[verify]**, NREL/PNNL published
  profiles) usable for annual-energy-intensity and shape checks per building type. MVP can start with a small curated set.
- **FR-CAL-4 (LATER) Tuning.** Automatic adjustment of sampling weights/scalars to fit measured data (scikit-learn /
  scipy optimization). MVP records the interface only; comparison is manual and read-only.
- **FR-CAL-5 (LATER) PJM node data.** Use PJM load-area or node data as a calibration reference.

### 6.5 Platform, API and quality — `PLAT`
- **FR-PLAT-1 (MUST)** Versioned REST API (`/api/v1`), OpenAPI docs, modular routers; the engine is a separate Python
  library that the API calls through one facade.
- **FR-PLAT-2 (MUST)** Long runs execute on a background worker queue, survive API restarts, report progress, are
  cancellable, and never block the API.
- **FR-PLAT-3 (MUST)** Notebook-first: the engine installs as a package (`pip install -e .`) and every key function is
  callable from an IPython notebook; example notebooks are maintained and tested.
- **FR-PLAT-4 (MUST)** One command starts the whole stack locally (Docker Compose); one command runs all tests.
- **FR-PLAT-5 (MUST)** CI runs lint, type-check, tests and coverage on every push; lint failures block merge.
- **FR-PLAT-6 (SHOULD)** Public demo deployed with HTTPS, rate limiting, spend cap/alerts, and no secrets in the repo.
- **FR-PLAT-7 (SHOULD)** Public repo readiness: license (owner decision), README that works on a clean machine,
  CONTRIBUTING, no secrets or large data in history, data attribution notices.

## 7. Non-functional requirements

| Area | Requirement |
|---|---|
| Performance | A 50-building, 100-variation DC run completes in **minutes** on the owner's laptop with visible progress; chart interactions < 300 ms for 8,760 points and acceptable for 35,040. Targets to be measured in Sprint 1, then fixed. |
| Resources | Fits the owner's **MacBook Air M2, 24 GB RAM, ~30 GB free disk**: Docker images + data + DB combined within budget; streaming/chunked reads; bounded caches. |
| Replayability | Every run stores config snapshot, seeds, engine version, data manifest hash, so any run can be replayed exactly; new runs default to fresh random seeds. |
| Security | Hashed passwords, JWT with short expiry, role checks in API, input validation, upload size/type limits, rate limiting on public endpoints, secrets only via environment. |
| Privacy | Uploaded measured data is private to its owner; never used in the public demo without explicit consent. |
| Observability | Structured logs with run ids; basic health/ready endpoints; error tracking is SHOULD. |
| Accessibility/UX | Keyboard-operable forms, color-blind-safe chart palette, responsive down to tablet. |
| Licensing/attribution | ResStock/ComStock, footprint, and weather data licenses and attribution displayed in-app and in the repo **[verify each]**. |
| Documentation | Every module has a docstring-level purpose; architecture and ADRs current; a newcomer can read the code without help. |

## 8. Quality standards

- **Test-driven development is mandatory:** a failing test precedes each behavior; no behavior merges without a test.
- Test pyramid: unit (engine, API), integration (API + DB + worker on fixtures), contract (OpenAPI), end-to-end (browser
  happy path), plus **scientific tests**: determinism, conservation of energy, known-answer neighborhoods,
  convergence on synthetic data, benchmark-range checks.
- Coverage gates: engine ≥ 90%, API ≥ 85%, critical web components ≥ 80% (enforced in CI once each exists).
- Static quality: ruff, black, mypy (strict on engine core), ESLint, TypeScript strict.
- Semantic versioning for the engine package, API (`/v1`), and web app; Conventional Commits; CHANGELOG; ADRs for every
  consequential decision.

## 9. Extension points the MVP must preserve (do not build)

Weather normalization using Kissock-style multivariate change-point models fitted per building or group, driven by
future or typical weather · EV / heat-pump / efficiency / solar scenarios from ResStock/ComStock upgrade datasets ·
submeters, virtual power plant and virtual net metering, aggregate point-of-interconnection meter · network as
one-line diagram; capacity checks; power flow (PyPSA / pandapower **[verify]**) · DER sizing and dispatch; REopt/SAM
input/output files; economics (PJM energy, capacity, ancillary) · **policy constraints** (e.g. a utility's maximum
microgrid size, PJM resource limits) as checkable rules · campus multi-meter aggregation · SEED storage · additional
states · Parquet export · full admin, billing, subscriptions · AWS hosting at scale.

## 10. Success criteria (30 days)

1. **Core demo (failure if missing):** from the web app, a Client defines a composition, runs it with progress and
   convergence, and views all required charts and downloads CSVs — on real DC data on the owner's machine.
2. Calibration screen compares an uploaded CSV with synthetic output and shows the metrics.
3. Three roles enforced; the Demo account is restricted to predefined compositions.
4. A public link serves the constrained demo with HTTPS and spend protection.
5. Test suite and CI green; docs sufficient for a newcomer; determinism golden test passing.
6. SHOULD: minimal map; public repository.

## 11. Assumptions, risks and open decisions

**Assumptions:** owner has the DC data locally (size/completeness not yet confirmed — Open Item O1); owner reviews at sprint
boundaries; cloud agents work on fixtures only unless data is made available.

| # | Risk | Mitigation |
|---|---|---|
| R1 | Scope far exceeds 30 days | Riskiest items first; week-2 checkpoint; map and tuning are the first to slip. |
| R2 | Real data can't be used in the cloud | Fixtures; local-only integration tests marked and documented. |
| R3 | Convergence definition is scientifically weak | Owner-approved criteria (O3); tests on synthetic ensembles. |
| R4 | Untrusted numbers published | Visible caveat; calibration screen; benchmark checks; honest messaging (FR-WEB-8). |
| R5 | Public link abused or costly | Locked demo role, precomputed results, rate limits, spend cap. |
| R6 | 30 GB disk exhausted | Raw Parquet outside the DB; image and cache budgets; ingestion reports disk use. |
| R7 | Data licensing/attribution missed | Per-dataset license review before public release. |
| R8 | Dormant/ignored config silently misleads | FR-ENG-3 (strict config) in Sprint 1. |

**Open items (owner input needed):** O1 DC data size/completeness (parked) · O2 footprint source choice · O3 convergence
criteria · O4 public repo license · O5 hosting provider and monthly budget · O6 week-2 rule if the date is at risk
(default: owner decides at the checkpoint) · O7 confirm "CBEX" means CBECS · O8 name of the product.
