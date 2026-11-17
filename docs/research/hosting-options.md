# Hosting options for the public demo and later production

Status: analysis only. This note recommends no spending; any spending needs the owner's approval first (CLAUDE.md, FR-PLAT-6).
Written 2026-11-11. "Accessed" = 2026-11-11 for every source below.

## Read this first: how reliable are the prices?

- **Prices change, and I could not read any provider's own pricing page.** The sandbox blocked `aws.amazon.com`, `hetzner.com`, `digitalocean.com`, `render.com`, `fly.io` and `railway.com` (DNS failure). Every price below comes from a web-search summary of a third-party blog, calculator or aggregator, and those sources disagree with each other in several places (noted per row). So every number here is **[verify]** on the provider's own page before any decision. Where I found no number I say so and do not guess.
- "Source says" = reported by the cited page. "Derived" = my arithmetic from sourced numbers. "Inference" = my judgement.
- Sizing numbers (worker memory, profile library size) are the owner's/PRD's estimates (FR-DATA-9: library roughly 0.1-0.5 GB; worker maybe 2-8 GB). Nothing has been measured on a server. Sprint-1 run-cost measurements should replace them.

## What has to be hosted

| Component | Needs (from PRD / architecture section 4) | Notes |
|---|---|---|
| React front end | static files | cheapest piece; any static host or CDN |
| FastAPI | small, always on | light CPU/RAM |
| Celery worker | CPU-heavy, 2-8 GB RAM, loads a 0.1-0.5 GB profile library | the main sizing driver |
| PostgreSQL + PostGIS + TimescaleDB | geometry, runs, users, calibration series | extension support decides managed vs self-managed |
| Redis | Celery broker/cache | small (tens of MB for this use; inference) |
| Domain, TLS, monitoring, backups | part of the $100 total | |

The raw 10 GB Parquet does not go to the server (FR-DATA-9). Development stays on the owner's MacBook (arm64, Apple silicon).

## Cross-cutting findings

**Managed Postgres and extensions**
- TimescaleDB on managed Postgres is the sticking point. Source says: DigitalOcean community answer states TimescaleDB is available as an extension on DO Managed Databases; a DO feature request says `timescaledb_toolkit` is missing; PostGIS support on DO managed Postgres was not confirmed in my results. (https://www.digitalocean.com/community/questions/timescale-db-in-managed-pgsql ; https://docs.digitalocean.com/products/databases/postgresql/details/supported-extensions/) [verify both extensions on the live page]
- Amazon RDS: I found no RDS documentation in my results. My recollection is that RDS offers PostGIS but not TimescaleDB; this is unconfirmed. [verify against the AWS RDS PostgreSQL extensions list]
- Timescale's own managed service (Tiger Data / Tiger Cloud) was not priced here. [verify if wanted]
- Licence: TimescaleDB has an Apache-2 edition (unrestricted) and a Community Edition under the Timescale License (TSL) containing compression and continuous aggregates; a forum post says TSL prevents third parties from reselling it as a service. Self-hosting for your own product is probably fine but the TSL text on commercial use was not read. (https://www.tigerdata.com/docs/about/latest/timescaledb-editions ; https://www.tigerdata.com/legal/licenses) [verify tsl/LICENSE-TIMESCALE before selling]
- Inference: a public demo of precomputed or capped runs may not need TimescaleDB at all (plain Postgres + PostGIS could serve it); this would widen the managed options. Needs a design check with the architect, not decided here.

**arm64 vs amd64.** The M2 laptop is arm64, so local Docker images are arm64. A server of another architecture needs multi-arch builds (`docker buildx`) or an arm64 host. Arm hosts mentioned in sources: AWS t4g (Graviton), Hetzner CAX. Whether the TimescaleDB+PostGIS image and every Python wheel (numpy, pyarrow, scipy) exist for both architectures was not checked. [verify]

**Egress.** AWS: first 100 GB/month to the internet free across services, then about $0.09/GB for the next tier; CloudFront: 1 TB/month free, no longer limited to the first year (source: AWS announcement via search, https://aws.amazon.com/blogs/aws/aws-free-tier-data-transfer-expansion-100-gb-from-regions-and-1-tb-from-amazon-cloudfront-per-month/ ; https://blog.besharp.it/aws-data-transfer-costs-in-a-nutshell/). Lightsail overage $0.09/GB in most regions (one source, https://techoral.com/aws/aws-lightsail-guide.html). Hetzner US locations: about 20% higher price and as little as 1 TB traffic vs 20 TB in Germany (aggregator; https://cloudpricecheck-com.pages.dev/hetzner/cloud-servers-pricing). Railway egress $0.05/GB in most sources, $0.10 in others (https://www.activepieces.com/blog/railway-pricing-2026-plans-usage-rates-and-costs). Fly.io egress sources conflict ($0.02/GB after 100 GB vs other figures). For this app egress is small: results are chart data of kilobytes to a few MB per run (inference), so egress is not a main cost driver unless abused.

**Backups.** Self-managed means the owner must set up `pg_dump`/WAL backups to a separate bucket; managed offers automatic backups (included or extra, price not verified). Provider snapshot prices were not found. [verify]

## Provider comparison

Prices are monthly USD, Linux, as reported. All [verify].

| Provider | What I found (source) | Fit / notes |
|---|---|---|
| **AWS EC2 (us-east-1)** | t4g.medium (2 vCPU, 4 GB) about $24.53; t4g.large (2 vCPU, 8 GB) about $49.06 on-demand (https://calculator.holori.com/aws/ec2/t4g.large/us-east-1, third-party). Spot figures also appear and are not suitable for a service. t4g are burstable Arm instances; sustained CPU use can exhaust credits or incur extra charge depending on credit mode [verify]. EBS storage price not found. | Flexible, most knobs, highest operational burden for a non-expert. |
| **AWS RDS** | db.t4g.medium (2 vCPU, 4 GB) about $47/month on-demand (https://www.bytebase.com/dbcost/rds/instance/db.t4g.medium/ ; $0.065/h at holori), single- vs multi-AZ basis not confirmed. Storage and backup extra, not found. | Alone nearly doubles the cost of one box; TimescaleDB unconfirmed. Not recommended under this budget (inference). |
| **AWS ElastiCache** | Price not found. | Skip; self-run Redis in a container. |
| **AWS Lightsail** | Smallest plan $3.50 (IPv6-only) / $5 with IPv4; a 4 GB plan "Medium" listed at $20 with 4 TB transfer (one source, https://techoral.com/aws/aws-lightsail-guide.html). No 8 GB plan price found. Check with `aws lightsail get-bundles`. | Simple flat pricing with generous transfer; upgrade path into AWS. |
| **AWS S3 + CloudFront** | CloudFront 1 TB/month free (above). S3 storage price not verified. | Right place for the static site and for the profile library file (inference). |
| **AWS budget tools** | Budgets alerts are free; first two action-enabled budgets free, later ones about $0.10/day (one source says $0.02); actions can stop EC2/RDS or change IAM when thresholds pass, but alerts and forecasts react to spend and are not a hard cap (https://aws.amazon.com/aws-cost-management/aws-budgets/pricing ; https://us.fitgap.com/products/aws-budgets). | Needs setup by the owner. |
| **Hetzner Cloud** | Conflicting. Reported 2026 price rises (June 15 wave: CX +31-38%, CAX +30-33%, per third parties) and reports that CX/CAX could not be ordered in Sept-Oct 2026; CX23 listed at 5.99 EUR with unclear date; aggregator lists CX22 from $5.60 in Ashburn (https://northflank.com/blog/hetzner-cloud-server-price-increases ; https://www.vincentschmalbach.com/hetzner-cheap-cloud-unavailable-price-increases/ ; https://cloudpricecheck-com.pages.dev/hetzner/cloud-servers-pricing). I found no reliable current price for a 4 vCPU / 8 GB size. | Historically cheapest, but availability and price are in flux; EU hosting adds latency for DC users (inference). Do not plan on it without a live check. |
| **DigitalOcean** | Droplets 4 GB about $24 and 8 GB about $48 (several blogs; one says $20 / $48, another a Premium tier $22 / $44; https://infratally.com/articles/digitalocean-droplet-pricing-guide-2026/). Managed Postgres from $15/month (1 GB, 10 GB disk); 4-8 GB tiers not found; per-minute billing reported for 2026 (https://docs.digitalocean.com/products/databases/postgresql/details/pricing/ ; https://infratally.com/articles/digitalocean-managed-postgres-deep-dive/). | Clear UI, managed DB with TimescaleDB reportedly available. |
| **Linode / Akamai** | Shared CPU 4 GB $24, 8 GB $48 (trackers; naming/tiers changed, a 4 GB dedicated listing of $36 also seen; https://costbench.com/software/cloud-infrastructure/linode/). Transfer allowance for these sizes not confirmed. | Similar to DO. |
| **Render** | Compute plans (renamed 26 Aug 2026): Starter $7 (512 MB), Standard $25 (2 GB, 1 CPU), Pro $85 (4 GB), Pro Plus $175 (8 GB); background workers start at Starter; Key Value 1 GB $20 after an October 2026 cut; Postgres prices conflicting (Basic 1 GB $19 per one calculator; others higher); workspace fee $25 or $29 per user (https://render.com/pricing.md ; https://usagepricing.com/blueprint/activity/render-2026-10-01-key-value-price-cut ; https://makerkit.dev/pricing-calculator/render). TimescaleDB on Render Postgres not confirmed. | Easy to operate; a 4 GB worker alone is $85, so the 4-component stack does not fit under $100 (derived). |
| **Fly.io** | Per-second machine billing; extra RAM $6/GB/month; shared-cpu-1x 1 GB about $5.70 (third party); volumes $0.15/GB/month; stopped machines pay only root-filesystem storage; Managed Postgres Basic 1 GB about $38 plus storage (derived by a third party); changes effective 2026-10-01 (https://fly.io/pricing-update/ ; https://kuberns.com/blogs/flyio-pricing/). | Can scale the worker to zero when idle; more DIY (volumes, networking). Moderate operational burden. |
| **Railway** | Hobby $5, Pro $20 (credit included); usage about $10/GB-month memory, $20/vCPU-month, volumes $0.15/GB-month, egress $0.05 or $0.10 per GB (sources conflict; https://www.activepieces.com/blog/railway-pricing-2026-plans-usage-rates-and-costs). | Usage billing suits spiky load, but a hard spend limit mechanism was not verified. [verify] |

Free tiers and credits: I did not find a reliable, current statement of free tiers or startup credits for any of these (only older or third-party mentions). Do not budget on them. [verify]

## Three sample configurations

All figures are monthly USD, derived by adding the sourced numbers above, and are ranges for "idle" (no traffic) vs "under load" (a demo audience running capped simulations). Domain, TLS and monitoring: I found no sourced prices. I assume a domain costs on the order of $1-2/month averaged (unsourced assumption, [verify]); TLS via a free certificate authority is common practice (not sourced); uptime monitoring and error tracking have free tiers at some vendors (not researched). Hence "+ domain/monitoring [verify]" rather than a number.

### Config A: One VPS, everything in Docker Compose (matches local setup)

- One 4 vCPU / 8 GB VPS from DigitalOcean or Linode (about $48, trackers), plus provider backups/snapshots (price not found), plus the static site on the same box via a reverse proxy, or on a free/cheap static host.
- Components: api, worker, db (Timescale+PostGIS image), redis, web, all on one machine, as in FR-PLAT-4.
- Idle: about $48 + backups + domain [verify]. Under load: the same, since CPU is fixed; the risk is the machine slowing down, not the bill rising. Egress overage at provider rates only above the included transfer.
- Pros: cheapest, closest to what the owner already runs; fixed bill. Cons: single point of failure; the owner manages OS updates and backups; the worker competes with the database for RAM.

### Config B: AWS, self-managed on EC2 with S3/CloudFront

- One EC2 t4g.large (about $49.06) for api, worker, db and redis in containers; S3 + CloudFront for the static front end and downloadable profile library (CloudFront 1 TB/month free; S3 storage price [verify]); EBS volume, snapshots, Budgets actions.
- Idle: about $49 + EBS/S3 (small, unpriced) + domain [verify]. Under load: about the same plus egress above the free allowances (roughly $0.09/GB beyond the free tier); burst-credit effects on a t4g under sustained CPU [verify].
- Pros: room to grow into RDS/auto scaling later; strong billing tools (alerts, actions). Cons: more to learn; billing is metered per service, so mistakes are easier than with a flat VPS; Budgets cannot guarantee a hard cap.

### Config C: PaaS (Render)

- Static site, an API Standard instance ($25), a worker Standard (2 GB, $25) or Pro (4 GB, $85), Key Value 1 GB ($20), plus Postgres (price unconfirmed; TimescaleDB unconfirmed).
- Derived: with a 2 GB worker, $25 + $25 + $20 = $70 before the database and workspace fee, so about $90-100+ and no headroom; with a 4 GB worker, $155+ which breaks the ceiling. Idle equals load for always-on services.
- Pros: least operational work. Cons: does not fit the budget or the 4 GB worker; PostGIS/TimescaleDB support not verified.
- A Fly.io variant could cost less by stopping the worker when idle (RAM $6/GB, so a 4 GB machine is roughly $24 if left on 24/7, derived from third-party figures) but is more DIY and its figures conflict. [verify]

## Recommended starting configuration (analysis, not a purchase)

Config A, a single VPS with Docker Compose, from a provider whose current 4 vCPU / 8 GB price and arm64/amd64 availability are confirmed live, with the static front end on a free or near-free static host or CDN. Reasons (inference): fixed monthly bill, the same Compose file as local development (FR-PLAT-4), no managed-service extension questions, and it leaves most of the $100 ceiling free for backups and growth. Start at 4 GB if measured run memory allows. Before the owner chooses: measure worker memory and run time on the laptop in Sprint 1 and confirm the profile library size.

Fallback if the owner wants less server administration: PaaS is likely out on cost; a managed DB on DigitalOcean is the one managed option with reported TimescaleDB support.

## Safeguards so spend cannot exceed $100

1. **Prefer fixed-price plans** (VPS flat monthly) over metered services; they cap the bill structurally.
2. **Provider spend alerts** at 50%, 80% and 100% of $100, to the owner's email and phone. On AWS, Budgets alerts are free; Budgets actions can stop instances but do not guarantee a cap (sourced above).
3. **A prepaid or capped payment method** (a virtual card with a monthly limit) if the provider allows; availability varies [verify].
4. **No unbounded autoscaling, no pay-per-request services** without a limit; no managed services the owner did not choose.
5. **Application limits** (FR-PLAT-6, section 4 of the PRD): hard caps on buildings, variations and run time; rate limits per IP/user; a queue with a maximum length; request size limits; no uploads; CAPTCHA or login for live runs [inference: CAPTCHA not in PRD].
6. **Kill switch:** one environment flag or command that disables live runs and serves the static results only; documented in a runbook; also a CDN/firewall rule to cut traffic. Test it before launch.
7. **Egress watch:** alert on transfer above a threshold, since egress is the only way a flat-price VPS can add cost.
8. **Monthly review** of the bill during the first three months.

## How the Demo role changes sizing

(Inference from the PRD's three-role design and FR-PLAT-6.)

- **Predefined compositions** can be precomputed once and served from the database or a file: no worker needed for most visits, which makes the static fallback and cache hit rate the main cost lever.
- **Capped live runs** bound the worker: with, say, a maximum number of buildings and variations per run and one run at a time per visitor, a 2-4 GB worker and a queue length limit probably suffice. The cap values are not chosen in this note; the sized numbers must come from Sprint-1 measurements (PRD: a 50-building, 100-variation DC run should take minutes on the laptop).
- Public traffic is a few concurrent users at most for a demo; the queue, not more hardware, should absorb bursts.
- The raw 10 GB Parquet and heavy analytics stay off the server (FR-DATA-9); only the compact library and the database ship.

## Fallback: static, precomputed demo only

| | Static precomputed | Live capped |
|---|---|---|
| Hosting | static site on CDN/object storage; AWS CloudFront 1 TB/month free (sourced); other free static hosts exist [verify prices] | VPS or PaaS above |
| Cost | likely a few dollars per month or less (inference; not priced) | about $48-100 |
| Risk of cost overrun | minimal | bounded by safeguards |
| Server administration | none | OS, backups, patching |
| What visitors can do | browse precomputed compositions and charts | change inputs, run new simulations |
| Security surface | very small | API, auth, rate limits |

FR-PLAT-6 already names the static site as the fallback; it is the right first public step if the owner wants a link before the live path is ready (inference).

## Decisions needed from the owner

1. Is a public link wanted at all for now (stretch goal), and if so, static first or live capped?
2. Preferred approach to administration: a single server the owner (or an agent) maintains, or the least effort at higher cost?
3. Any provider or region preferences (for example US East for DC users, or a provider the owner already uses)?
4. Is a $100 ceiling meant to include a safety buffer, or should the plan target a lower steady-state (for example $50-60)?
5. Approval to do a live price check (provider pages on a machine with open internet) and a Sprint-1 sizing measurement before any choice.
6. Is TimescaleDB required on the public server, or may the demo run on plain Postgres + PostGIS? (affects managed-database options)
7. Who holds the cloud account and billing, and may a spending-capped card be used?
8. Domain name: does the owner already have one?

## Still unverified

- Every provider price and free tier (no primary pricing page was readable); Hetzner availability; RDS and ElastiCache prices; S3 and EBS prices; domain and monitoring prices.
- TimescaleDB and PostGIS support on each managed Postgres offering; arm64 images for the full stack.
- The TSL licence text on commercial self-hosting.
- Worker memory and run time on a server, and the profile library size.

## Sources (accessed 2026-11-11; all via search summaries, none opened directly)

- AWS EC2 t4g: https://calculator.holori.com/aws/ec2/t4g.large/us-east-1 , https://calculator.holori.com/aws/ec2/t4g.medium/us-east-1
- AWS RDS: https://www.bytebase.com/dbcost/rds/instance/db.t4g.medium/ , https://aws.amazon.com/rds/postgresql/pricing
- AWS egress and CloudFront: https://aws.amazon.com/blogs/aws/aws-free-tier-data-transfer-expansion-100-gb-from-regions-and-1-tb-from-amazon-cloudfront-per-month/ , https://blog.besharp.it/aws-data-transfer-costs-in-a-nutshell/
- AWS Budgets: https://aws.amazon.com/aws-cost-management/aws-budgets/pricing , https://us.fitgap.com/products/aws-budgets
- Lightsail: https://techoral.com/aws/aws-lightsail-guide.html , https://dev.to/vamsi_0fe326ce8584827fca5/aws-lightsail-pricing-in-2026-a-complete-breakdown-before-you-commit-3n66
- Hetzner: https://northflank.com/blog/hetzner-cloud-server-price-increases , https://www.vincentschmalbach.com/hetzner-cheap-cloud-unavailable-price-increases/ , https://cloudpricecheck-com.pages.dev/hetzner/cloud-servers-pricing
- DigitalOcean: https://infratally.com/articles/digitalocean-droplet-pricing-guide-2026/ , https://docs.digitalocean.com/products/databases/postgresql/details/pricing/ , https://docs.digitalocean.com/products/databases/postgresql/details/supported-extensions/ , https://www.digitalocean.com/community/questions/timescale-db-in-managed-pgsql
- Linode/Akamai: https://costbench.com/software/cloud-infrastructure/linode/
- Render: https://render.com/pricing.md , https://makerkit.dev/pricing-calculator/render , https://usagepricing.com/blueprint/activity/render-2026-10-01-key-value-price-cut
- Fly.io: https://fly.io/pricing-update/ , https://kuberns.com/blogs/flyio-pricing/
- Railway: https://www.activepieces.com/blog/railway-pricing-2026-plans-usage-rates-and-costs
- TimescaleDB editions/licence: https://www.tigerdata.com/docs/about/latest/timescaledb-editions , https://www.tigerdata.com/legal/licenses
