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
- **G6** Ship a complete demo on the owner's machine by day 30; stretch: a public, constrained live demo, and (nice to have) a public repository.

### 3.2 Non-goals for the 30-day MVP
Power-flow physics, capacity checks, DER sizing and dispatch, revenue/economics, REopt/SAM file export, weather-driven
forecasting, EV/heat-pump scenarios, multi-region data, billing, full user administration,
arbitrary GIS uploads, open public simulation. These are **LATER** — see §9 for the hooks the MVP must leave.
Separately, heavy industrial/manufacturing buildings are **out of scope** (not merely later) unless ComStock defines them;
community-scale warehouses are **in scope** (FR-ENG-1).

## 4. Users and access tiers

| Tier | Who | Capabilities (MVP) |
|---|---|---|
| **Admin** | Owner | Everything: define any parameter, **use and configure attribute filters**, manage compositions and demo content, view calibration and diagnostics, configure the demo. |
| **Client** | Owner acting as first customer; later paying users | Full flexibility to define neighborhoods and parameters, run, view, download, upload calibration data. |
| **Demo** | Anonymous public visitor | Locked down: choose from **predefined compositions** only and **run them live** with hard caps (small building count, low `max_runs`, queue, rate limit, one run at a time per visitor); results can be cached by (composition, seeds). Charts and downloads available. No uploads, no filters, no free-form compositions. Seed behavior (fixed stored seeds by default; optional "draw a new random sample" button) to be confirmed by the owner. |

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
- **Run** — one execution of an ensemble for a composition and settings; replayable by stored seeds + version.
- **Weather year** — AMY2018 datasets represent buildings under the *actual weather of 2018*, not a long-run typical year (TMY3). The MVP's "typical year" is therefore "the 2018 weather year"; the UI and exports must label it that way (FR-WEB-8). Genuine typical-year results would need TMY3 datasets or the weather-normalization work in §9.
- **Dataset / scenario** — a ResStock or ComStock data package identified by release, weather version, upgrade id and state;
  the baseline is the default scenario, and upgrade scenarios simulate other configurations (e.g. high heat-pump or EV penetration).

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
- **FR-ENG-6 (MUST) Adaptive convergence** *(owner approved the proposed default 2026-11-06, provisional, revisit after first real runs)*. Run up to `max_runs` (default 1,000), stopping early when convergence
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
- **FR-ENG-11 (MUST) Attribute filtering of source buildings, admin-configured.** For every composition category, the
  user can filter the ResStock/ComStock building population by building characteristics. The mechanism has two layers:
  1. **Discovery (automatic):** after ingestion, the system knows every characteristic column in the data, its type, and
     its value summary (distinct values or min/max/count).
  2. **Exposure (admin-configured):** an admin chooses which attributes users may filter on, with a friendly label,
     help text, allowed operators, value choices or numeric bounds, which building types it applies to, and its
     display order. The composition form is built from this configuration, not from code.
  **Starter set (MVP defaults, editable by the admin):** stories/height; total dwelling units (multifamily only);
  attached vs detached (single-family); EV present; and a few more chosen from the data in Sprint 1, for example year
  built/vintage, floor area, heating fuel/system, and heat-pump presence **[verify column names in the DC data]**.
  AC:
  (a) a filter language supports equals/in-list, numeric ranges, not-null, AND-combination across attributes;
  (b) the exposed-attribute configuration is stored in the database, versioned, and editable by an Admin through the
      API (UI for it is FR-WEB-9); **in the MVP the filter feature is Admin-only** — Clients and Demo do not see or use
      filters. Access is governed by a per-role **permission flag** (e.g. `filters.use`), not hard-coded role checks, so
      enabling it for Clients later is a configuration change (when enabled, Clients see only exposed attributes; Demo
      never gets it). Compositions created by an Admin with filters remain runnable by others as saved/predefined
      compositions;
  (c) a **pool preview** reports, per category, how many buildings match and warns on zero, or fewer than a configurable
      minimum (default 10), since sampling with replacement from a tiny pool understates diversity;
  (d) an attribute that is not in the data, or an invalid value, is a clear error (consistent with FR-ENG-3);
  (e) the effective filters and matched pool size are stored with every run and shown in results;
  (f) changing a filter changes only the sampled pool, never silently the methodology.
  Dependencies: FR-ENG-1 (registry types carry default filters), FR-ENG-3 (no silently ignored keys), FR-DATA-7
  (characteristics available for fast filtering), FR-WEB-1 (filter UI), FR-WEB-9 (admin screen), FR-WEB-7/DATA-6
  (mapped buildings' stories/units/year built can seed filter values), FR-CAL (calibration may constrain pools).
- **FR-ENG-12 (LATER; design now) Scenario switching and mixing.** ResStock and ComStock also simulate other
  configurations (upgrades such as heat pumps, EV charging, efficiency measures) as separate datasets. The engine must
  be able to draw each sampled building from the **baseline or from a scenario dataset** according to a **scenario
  rule**. Example: when a target characteristic penetration (e.g. share of homes with heat pumps or EV charging)
  reaches a threshold, draw that share from the matching upgrade dataset and the rest from baseline; or switch a whole
  category to a different dataset. How penetration maps to datasets, how thresholds are defined, and how consistency
  is kept (e.g. pairing the same building across baseline and upgrade) are **methodology decisions for the owner**
  **[verify with ResStock/ComStock documentation how upgrade files relate to baseline buildings]**. MVP obligations
  (so this is not a refactor later): dataset ids as inputs (FR-DATA-8); a `scenario` field in the composition schema
  that defaults to baseline; the pool builder keyed by dataset; run records that store which dataset each sampled
  building came from. AC (MVP part): a test builds pools from two fixture datasets labeled baseline and "upgrade" and
  the run record shows per-building dataset ids, even though no UI exposes it yet.
- **FR-ENG-13 (LATER; design now) Weather normalization before weather-driven use.** The datasets are AMY2018: each
  building's load reflects actual 2018 weather. Before any load is driven by other weather (future years, TMY, or
  actual observed weather), the loads are **weather-normalized**: for each building (or building group), fit a
  **multivariate change-point regression** (Kissock-style; variants such as 3-, 4- or 5-parameter models, with
  temperature and optionally other weather variables and time-of-day/day-type terms) of load against the **2018
  weather the dataset was simulated with**; judge fit quality (e.g. R², CV(RMSE)); derive normalized annual
  consumption patterns; then predict load under the target weather. Hourly/15-minute shape handling, grouping choices,
  fit thresholds, and which weather variables to use are **methodology decisions for the owner**. Only after
  normalization may any weather-driven forecast be produced. MVP obligations so this is not a refactor:
  (a) the weather series used by each dataset is ingested, catalogued and stored with the dataset (FR-DATA-8,
  FR-ENG-10) **[verify where NREL publishes the AMY2018 weather files for each release]**;
  (b) building-level hourly/15-minute load is retrievable per sampled building, not only as neighborhood totals;
  (c) the engine has an empty `weather` module with a documented interface (fit, predict, quality report);
  (d) the same module can later weather-normalize measured/calibration data so calibration compares like with like.
  Known limits to state in the docs: one weather year gives limited coverage of extremes, and fits on simulated loads
  describe the simulation model, not necessarily real building behavior.

### 6.2 Data — `DATA`
- **FR-DATA-1 (MUST) DC ingestion.** A scripted, resumable path to provision (data root and dataset choice are **configurable** via an environment variable/config, never hard-coded paths) DC ResStock/ComStock inputs into the
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
- **FR-DATA-7 (MUST) Building characteristics store.** The ResStock/ComStock building-characteristics tables (one row per
  building, one column per characteristic) for the ingested region are loaded into PostgreSQL (plain tables, indexed on
  commonly filtered columns) so filtering and pool previews are fast and queryable from the API. Raw 15-minute Parquet
  stays on disk. AC: loading is repeatable from the manifest; a pool preview over DC returns in well under a second;
  column names and value meanings are documented in a generated data dictionary shown in the UI.
- **FR-DATA-8 (MUST) Dataset identity and catalog.** A "dataset" is identified by **(product ResStock|ComStock, release,
  weather version e.g. TMY3|AMY2018, upgrade/scenario id, state)**. The system keeps a **dataset catalog** (database
  table plus data dictionary per release) recording for each: location on disk, manifest hash, size, license, and a
  human label and scenario family for the scenario (e.g. "Baseline", "Cold Climate Air-Source Heat Pump"; the owner-supplied
  ResStock list is in [`reference/resstock-upgrade-measures.md`](./reference/resstock-upgrade-measures.md), ComStock in
  [`reference/comstock-upgrade-measures.md`](./reference/comstock-upgrade-measures.md); ComStock scenario ids are strings, so the
  catalog keeps an internal scenario key plus the on-disk partition id). Each dataset also records whether its load is **gross demand
  or net of on-site generation** (some ComStock scenarios include PV/battery), and gross and net must never be mixed silently Every run stores the dataset ids it
  used. **MVP datasets (owner decision, 2026-11-06):** ResStock **2025 Release 1, AMY2018** and ComStock **2025 Release 3, AMY2018**
  (newest releases, shared 2018 weather year, full measure lists), DC only, subject to an agent confirming their OEDI
  folder names, DC coverage, and known issues; if the owner's local files come from other releases, the data must be
  re-ingested or the choice revisited with the owner. The MVP uses the **baseline** scenario only, currently believed to be `upgrade=0` **[verify in each release's
  documentation/data dictionary: upgrade numbering and meaning are release-specific]**. AC: the catalog can describe
  more than one upgrade for the same state; the engine takes dataset ids as inputs rather than hard-coded paths;
  ingesting an extra upgrade adds catalog rows without code changes; ingestion shows progress and disk use.
- **FR-DATA-9 (SHOULD) Compact building-profile library.** Live computation is cheap if each run does not have to read
  large raw files. A build step extracts, for each ingested building, the needed load series (and its attributes) into a
  compact, versioned library (for example one columnar file per dataset at float32), from which the engine samples
  and sums. The public server then needs this library and not the raw 15-minute Parquet. AC: results from the library
  match results from the raw files within a documented tolerance on fixtures; library size and load time are reported;
  redistribution is permitted by the datasets' licenses **[verify]**.

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
- **FR-WEB-9 (SHOULD) Admin console.** Manage demo compositions and the demo account's allowed options, and **configure
  which building attributes are exposed as filters** (FR-ENG-11). If the screen slips, the same configuration is edited
  through the API or a versioned config file; the underlying configuration (not the screen) is MUST.

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
- **FR-PLAT-6 (SHOULD) Public demo with live capped computation.** Development and the first demo run **on the owner's
  machine**; public hosting is decided later. Owner budget: **$100 per month total** for all hosting and services
  (ceiling, not authorization: any spending needs the owner's approval first). An agent produces a costed hosting
  recommendation (instance size from measured run cost, database, storage, domain, monitoring) **[verify current
  pricing]** at the Day-14 checkpoint, or later if the owner defers it. Safeguards whatever the host: hard caps (§4),
  rate limiting, queue, request size limits, HTTPS, no secrets in the repo, no uploads, spend alerts at the ceiling,
  and a kill switch. A static site of precomputed results remains the fallback. Nothing is published without the
  owner's approval.
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

Weather normalization (FR-ENG-13) using Kissock-style multivariate change-point models fitted per building or group on
the AMY2018 loads and weather, **before** loads are driven by future, typical or observed weather · EV / heat-pump / efficiency / solar scenarios from ResStock/ComStock upgrade datasets (design hooks: FR-DATA-8, FR-ENG-12) ·
submeters, virtual power plant and virtual net metering, aggregate point-of-interconnection meter · network as
one-line diagram; capacity checks; power flow (PyPSA / pandapower **[verify]**) · DER sizing and dispatch; REopt/SAM
input/output files; economics (PJM energy, capacity, ancillary) · **policy constraints** (e.g. a utility's maximum
microgrid size, PJM resource limits) as checkable rules · campus multi-meter aggregation · SEED storage · additional
states · Parquet export · full admin, billing, subscriptions · AWS hosting at scale.

## 10. Success criteria (30 days)

**Minimum viable goal (MUST): a complete demo on the owner's machine**, orchestrated with Docker Compose.
**Stretch goal (SHOULD): a public live link** with capped live computation, within the $100/month ceiling.

1. **Core demo (failure if missing):** from the web app, a Client defines a composition, runs it with progress and
   convergence, and views all required charts and downloads CSVs, on real DC data on the owner's machine via
   `docker compose up`.
2. Calibration screen compares an uploaded CSV with synthetic output and shows the metrics.
3. Three roles enforced; the Demo role is restricted to predefined compositions.
4. Test suite and CI green; docs sufficient for a newcomer; determinism golden test passing.
5. SHOULD: minimal map viewer; public repository (owner publishes on their own command).
6. STRETCH: public live link serving the constrained Demo role (FR-PLAT-6), published only with owner approval.

## 11. Assumptions, risks and open decisions

**Assumptions:** owner has the DC data locally (size/completeness not yet confirmed — Open Item O1); owner reviews at sprint
boundaries; cloud agents work on fixtures only unless data is made available.

| # | Risk | Mitigation |
|---|---|---|
| R1 | Scope far exceeds 30 days | Riskiest items first; week-2 checkpoint; map and tuning are the first to slip. |
| R2 | Real data can't be used in the cloud | Fixtures; local-only integration tests marked and documented. |
| R3 | Convergence definition is scientifically weak | Owner-approved criteria (O3); tests on synthetic ensembles. |
| R4 | Untrusted numbers published | Visible caveat; calibration screen; benchmark checks; honest messaging (FR-WEB-8). |
| R5 | Public link abused or costs exceed the $100/month ceiling | Locked demo role with hard caps, rate limits, kill switch; compact profile library (FR-DATA-9); static fallback (FR-PLAT-6). |
| R6 | 30 GB disk exhausted | Raw Parquet outside the DB; image and cache budgets; ingestion reports disk use. |
| R7 | Data licensing/attribution missed | Per-dataset license review before public release. |
| R8 | Dormant/ignored config silently misleads | FR-ENG-3 (strict config) in Sprint 1. |

**Open items (owner input needed):** O1 DC data size/completeness (parked) · O2 footprint source choice · O3 convergence
criteria (approved provisionally) · O4 public repo license · O5 hosting: budget ceiling $100/month total; provider chosen by agent recommendation after local MVP · O6 week-2 rule if the date is at risk
(default: owner decides at the checkpoint) · O7 confirm "CBEX" means CBECS · O8 name of the product.
