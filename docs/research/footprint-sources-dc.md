# Building footprint and attribute sources for Washington, DC

Status: research note for owner approval (FR-DATA-6). Analysis only; nothing has been downloaded, integrated or published.
Written 2026-11-11. All "accessed" dates below are 2026-11-11.

## How to read this note

- **Source says** = a statement taken from a search result or page I could read. **Inference** = my reading of it. **[verify]** = needs a live check before relying on it.
- **Access limits in this session.** The sandbox could resolve only a few hosts. I could NOT open `opendata.dc.gov`, `maps2.dcgis.dc.gov`, `catalog.data.gov`, `docs.overturemaps.org`, `wiki.openstreetmap.org`, or `otr.cfo.dc.gov` directly (DNS failure; they would need to be allowed in the session's network settings). I could read `github.com/microsoft/globalmlbuildingfootprints` directly. Everything about the DC, OSM and Overture sources therefore comes from search-result summaries, not from the primary pages. Treat field lists, licences and sizes as **[verify]** until someone opens the primary pages.
- I downloaded no data, so I give **no measured coverage numbers** (match rates, % with stories, file sizes). Where I give none, the cell says "not measured".

## What the project needs (from the registry)

`src/microgrid/community_load/building_types.yaml` selects a building type from a use class (`geo/types.py` `UseClass`), optional `subtypes`, and a story range. `src/microgrid/geo/classify.py` maps a raw label (lower-cased) through a fixed table (OSM `building=*` values plus common municipal words) and returns UNKNOWN for anything else; `AttributeMapping` names which property holds id, use, stories, height_m, units, year_built (defaults are OSM tag names: `building`, `building:levels`, `height`, `building:flats`, `start_date`).

Needed per building, in priority order:

| Need | Why | Registry rule |
|---|---|---|
| Use class | picks ResStock vs ComStock and the type | `sf_detached`, `sf_attached`, `mf_*`, `commercial_private`, `commercial_warehouse`, `institutional_school`, `institutional_hospital`, `civic` (inactive) |
| SF detached vs attached | two separate ResStock pools | needs a rowhouse/attached signal |
| Stories | splits multifamily into 1-3 / 4-7 / 8+; unknown falls back to 3 (`assumed_stories_when_unknown`) | `min_stories`/`max_stories` |
| Subtype (school, hospital, church, government, fire_station) | `institutional_school`, `institutional_hospital`, civic | `subtypes` |
| Floor area, units, year built | not used by the registry rules today; useful for scaling and sanity checks | none yet [inference] |

Note: classify.py has no mapping for DC CAMA numeric use codes or descriptions, so an adapter table is needed for whichever DC attribute source is chosen.

## Source-by-source

### 1. DC Open Data: Building Footprints 2023 (geometry)

| Item | Finding |
|---|---|
| Provides | Polygon footprints; per a 2021 table schema: objectid, gis_id, featurecode, description, captureyear, captureaction, shapearea, shapelen. No height, stories, use or units found in those field lists. (Source says, via a baselight mirror of the 2021 dataset; 2023 schema not seen directly. [verify]) |
| Capture rules | Roof-line outlines; structures of 100 sq ft (9.29 m2) or more, including sheds and garages; courtyards/atria not part of the building. (Source says: data.gov catalog entries.) |
| Coverage/freshness | Whole District. 2023 edition from the Office of the Chief Technology Officer; catalog metadata dates conflict (modified 2024-03-22 in the DCAT record, metadata update 2026-02-25 on data.gov). Older editions exist (2019, 2021) and a separate "Building Footprints" item. Which is newest is [verify]. |
| Licence | The catalog entry lists Creative Commons Attribution 4.0. The District's site terms page (search result) says data is public domain / CC0 and credit is requested not required, and notes dataset-specific terms may apply. These conflict; read the licence on the live dataset page. [verify] Either is permissive: no share-alike, commercial use allowed (inference from CC BY / CC0 text). A disclaimer of warranties/liability applies. |
| Formats / download | CSV, GeoJSON, KML, Shapefile and an ArcGIS REST service (source says). Plain HTTP download; no key known. Size not measured. |
| Join keys | `gis_id` joins to the DC Unique Building Identifier (UBID) table, which also carries `ssl` (Square-Suffix-Lot) (source says; it was built by spatially joining 2019 footprints to Common Ownership Lots). So footprint -> SSL -> CAMA is possible without a spatial join, but the UBID table is based on 2019 footprints, so 2023 footprints need a fallback spatial join (inference). [verify] |
| Quality caveats | Footprints include sheds/garages (OTHER class); a building on several lots, or a lot with several buildings, makes the SSL join many-to-many (inference). |
| Effort | Low for geometry (GeoJSON into the existing loader). Medium for the attribute join. |

### 2. DC property/assessment attributes (CAMA, Office of Tax and Revenue)

Note on "DC OpenGov": I found no dataset or portal by that name. The owner most likely means Open Data DC (opendata.dc.gov) and/or the OTR CAMA datasets below. Confirm with the owner.

Datasets (source says, catalog.data.gov / DC GIS REST): Computer Assisted Mass Appraisal (CAMA) Residential, Commercial, and Condominium, plus "Integrated Tax System Property Use Codes" and "Tax System Property Sales (CAMA)". The DC GIS Property_and_Land service lists Residential (CAMA) as layer 25 and Condominium as layer 24.

| Item | Finding |
|---|---|
| Provides (Residential layer) | Fields seen: SSL, USECODE (Property Use Code), NUM_UNITS (Number of Units), STORIES, AYB (Average Year Built), EYB (Improvement Year Built), GBA (Gross Building Area). A LIVING_GBA field was not seen. (Source says: DC GIS layer 25 page via search summary; full field list not read. [verify]) Commercial and Condominium field lists not read. [verify] |
| Use class | USECODE is numeric; the code table is a separate dataset ("Integrated Tax System Property Use Codes") and part of OTR's Assessor Reference Materials. One old OTR document suggests single-family codes sit in 11-19 and 23-24 and multifamily in 21-22 and 25-29. That is from a single 2000s-era document; treat as [verify] and do not hard-code it. |
| Coverage/freshness | Assessed real property citywide; sales data is updated daily (snapshot from the CAMA system); the assessment roll itself is annual (inference). Tax-exempt property (schools, federal, churches, government) may be absent or thinly attributed in CAMA; this is an inference from how assessment rolls generally work and must be checked, because it affects schools, hospitals and civic buildings. [verify] |
| Licence | Catalog says CC BY 4.0 with a disclaimer that the data is for assessment purposes (source says). Permissive; no share-alike. See the CC0-vs-CC BY note above. [verify] |
| Formats / download | ArcGIS GeoService, CSV, File Geodatabase, GeoJSON, KML, Shapefile (source says). Size not measured. |
| Join keys | SSL (Square, Suffix, Lot) is the primary key and the documented join key ("most useful when joined by SSL to other real property data"). |
| Quality caveats | Condominiums sit in a separate table (one record per unit), so unit counts and stories must be aggregated; CAMA records are per property record, not per footprint; a Berkeley archive item labeled "Residential" actually describes 2005 commercial data (mirror mislabel; use the DC portal only). Numeric fields are typed double, so expect 0 and null for unknown (inference). |
| Effort | Medium. Need: use-code lookup table, condo aggregation, SSL join, many-to-many handling. |

Other DC government layers worth a live check: Common Ownership Lots / Property Lots (parcel polygons with SSL), Address Points (MAR), and "District Government Owned Structures" (turned up in search). Names and availability [verify].

### 3. OpenStreetMap

| Item | Finding |
|---|---|
| Provides | Footprints plus optional tags: `building=*` (type), `building:levels`, `height`, `building:flats`, `start_date`. These are exactly the defaults in `AttributeMapping`, so the existing classifier is built for OSM. |
| Coverage in DC | A 2013-2014 import brought roughly 66,000 DC government footprints with addresses into OSM, largely replacing existing footprints; ways carry `dcgis:*` tags and some have `building:levels` (source says: OSM wiki import page via search; whether levels came from the import or later mappers is not confirmed). I found no statistic on what fraction of DC buildings have levels, use or flats. Not measured. Footprints are likely older than DC's 2023 data (inference). |
| Licence | ODbL 1.0 with attribution ("© OpenStreetMap contributors"). Share-alike applies to Derivative Databases; a Produced Work (for example a rendered chart or map) can carry any licence, but recipients may request the underlying data. Internal-only use triggers no sharing (source says: OSMF Licence and Legal FAQ, search summary). The grey area is where a product extracts or recreates substantial parts of OSM data, which counts as a Derivative Database (same source). Needs a lawyer's read before selling. [verify] |
| Download | Overpass API (small areas), Geofabrik extracts (PBF), or the OSM API. Not checked. [verify] |
| Join keys | None to DC parcels except addresses and, for imported buildings, `dcgis:*` tags. Otherwise a spatial join to parcels. |
| Quality caveats | Tag completeness is uneven; labels are free text (classify.py returns UNKNOWN for unknown values by design); stories often missing. |
| Effort | Low (parser matches the existing mapping). Attribute quality is the limit, not effort. |

### 4. Microsoft Global ML Building Footprints

| Item | Finding |
|---|---|
| Provides | Footprint polygons (ML-derived from imagery), a height estimate in metres (-1 when none), and a confidence score 0-1 for the footprint (older footprints use -1). No use, stories, units or year built. (Source says: GitHub README, read directly 2026-11-11.) |
| Coverage/freshness | Global (about 1.4B buildings per the README intro); frequent refreshes; latest README entry: global refresh 2026-08-13. Imagery vintage 2014 onward. Quality is weaker in dense urban areas; evaluation precision about 92-97%, recall about 70-86%; square-shaped gaps possible. |
| Licence | The README states CDLA Permissive 2.0 (read directly). A search result from the Planetary Computer catalog lists ODbL-1.0 for ms-buildings, which may describe an older US dataset or a stale entry. Conflict. [verify] CDLA Permissive 2.0 has no share-alike on the data (inference from licence family; text not read); ODbL would. |
| Format / download | Line-delimited GeoJSON, EPSG:4326, files named `.csv.gz` but containing GeoJSON lines; index file `dataset-links.csv` partitioned by country and quadkey; a separate US repo exists. Hosting URL changed 2026-07-24. DC is a few tiles; size not measured. |
| Join keys | None; spatial join to parcels/addresses only. |
| Effort | Low-medium (tile selection, clip to DC). Adds nothing the DC footprints lack except ML height. |
| Caveat | ML footprints are generally less precise than a municipal planimetric layer, and DC has an authoritative one (inference). |

### 5. Overture Maps buildings

| Item | Finding |
|---|---|
| Provides | Footprints with optional `height`, `num_floors`, `num_floors_underground`, `has_parts`, building parts (class and subtype value lists not read). Height and roof_height are capped at 850 m (source says: Overture schema pages via search). |
| Sources inside | OpenStreetMap, Esri Community Maps, Microsoft ML footprints, Google Open Buildings, merged in priority order with OSM first (source says: Overture docs and release notes). So for DC it is largely OSM (itself largely DC's old import) plus ML fill. It is not independent evidence of attributes (inference). |
| Coverage/freshness | Global, releases roughly monthly; the newest release I saw cited was 2026-08-19 (schema 1.18.0), with a later release path (2026-10-21.0) also seen; which is latest is [verify]. |
| Licence | Buildings theme is ODbL because it includes OSM; all other sources must be ODbL-compatible. Attribution text given in release notes: "Building data © OpenStreetMap contributors, Microsoft, Esri Community Maps contributors, Google Open Buildings." Overture's FAQ says ODbL themes are a Derivative Database of OSM (source says, search summaries). Share-alike risk is the same as OSM. [verify] |
| Format / download | GeoParquet on AWS S3 and Azure (path form `release/<version>/theme=buildings/type=building/`), queryable with DuckDB or the Overture CLI (CLI not confirmed). A bounding-box extract for DC should be small (inference). |
| Join keys | Overture id; no parcel key. Spatial join only. |
| Effort | Low-medium. |

## Comparison summary

| | DC footprints 2023 | OSM | Microsoft ML | Overture | DC CAMA (attributes) |
|---|---|---|---|---|---|
| Geometry | yes (authoritative, 2023) | yes (older, DC-derived) | yes (ML) | yes (merged) | no (parcel-keyed rows) |
| Stories | no | sometimes | no (height only) | sometimes | yes (STORIES) |
| Use class | no | sometimes | no | sometimes | yes (USECODE) |
| Units | no | rare | no | no | yes (NUM_UNITS; condo needs aggregation) |
| Year built | no | rare | no | no | yes (AYB/EYB) |
| Floor area | shape area only | no | no | no | yes (GBA) |
| Parcel key | via UBID (`gis_id` to SSL) [verify] | no | no | no | SSL |
| Licence | CC BY 4.0 or CC0 [verify] | ODbL | CDLA-P 2.0 or ODbL [verify] | ODbL | CC BY 4.0 or CC0 [verify] |
| Share-alike risk | none | yes | only if ODbL | yes | none |

Registry fit (inference from the field lists above):

- **sf_detached / sf_attached:** CAMA USECODE plus building style; detached vs rowhouse is not obvious from the fields I saw and may need other CAMA fields or footprint adjacency (shared walls). [verify field list]
- **mf_small/mid/large:** CAMA STORIES + NUM_UNITS; condos need aggregation.
- **commercial_private, warehouse:** CAMA Commercial layer use codes [verify fields].
- **school, hospital, civic:** most exposed. Tax-exempt parcels may not be in CAMA; DC government layers (for example "District Government Owned Structures") and OSM tags are fallbacks. [verify]

## RECOMMENDATION (for owner approval; nothing adopted)

1. **Geometry:** DC Open Data Building Footprints 2023 (or the newest edition on the portal; confirm). Reasons: authoritative, citywide, permissive licence, no share-alike, linkable to parcels.
2. **Attributes:** DC CAMA Residential, Commercial and Condominium layers, with the OTR use-code table, for use class, stories, units, year built and gross area.
3. **Join:** Footprint `gis_id` -> UBID table -> SSL -> CAMA (SSL). For footprints missing from UBID (2023 vs 2019 base), spatially join the footprint centroid to the Common Ownership Lot polygon to get SSL. Aggregate condo records per SSL/footprint. Many-to-many cases need an explicit rule (for example allocate to the largest footprint) logged as a decision. [verify UBID currency]
4. **Institutional gaps:** fill schools, hospitals and civic from DC government layers, with OSM tags only as a labelled classification fallback (not redistributed).
5. **Not recommended as primary:** Microsoft (no attributes, lower precision where DC has better), Overture (ODbL, mostly OSM + Microsoft, nothing DC lacks). They could help find missing structures (inference).

**What is missing even with this choice:** (a) single-family detached vs attached; (b) subtype for school/hospital/civic where parcels are tax-exempt; (c) the civic type is inactive in the registry, so those buildings stay unmodeled; (d) building height in metres (stories only); (e) footprint-to-record uniqueness on multi-building lots; (f) a CAMA use-code to UseClass mapping table, which classify.py does not have (it returns UNKNOWN rather than guessing, which is the right behaviour). Also: the ResStock/ComStock samples are not tied to specific DC buildings, so the mapping is statistical, not per address (inference from the registry design).

## Licence risks for a commercial product

| Risk | Level | Note |
|---|---|---|
| DC footprints and CAMA | Low | Permissive (CC BY 4.0 or CC0 [verify]); attribute "Data from Open Data DC"; keep the District's disclaimer; do not imply District endorsement. CAMA records may include owner names/addresses: do not ingest or display owner fields (inference; privacy law not reviewed). |
| OSM | Medium-High | ODbL share-alike for a Derivative Database; required attribution. Safer pattern: show only Produced Works (charts, aggregated results) and do not redistribute extracted OSM building data. Needs legal advice before sale. |
| Overture | Medium-High | Same as OSM (ODbL derivative) and mixes sources. |
| Microsoft | Low-Medium | CDLA Permissive 2.0 per the README; the ODbL listing elsewhere must be resolved. |
| DC GIS REST service terms | Unknown | API terms and rate limits not read. [verify] |

I am not a lawyer; none of this is legal advice. The TimescaleDB and ResStock/ComStock licences are outside this note.

## Questions for the owner

1. "DC OpenGov": is that Open Data DC (opendata.dc.gov), or a different site or dataset you have in mind?
2. Are you willing to rely on tax-assessment data (CAMA) for building attributes, knowing that schools, churches and government buildings may be missing or thin?
3. Will you sell the software only, or also data/exports? If only results (charts, estimates) are sold, an ODbL source is easier to live with; if building data is exported, it is not.
4. How much does detached vs attached single-family matter for the first demo?
5. Should civic buildings stay unmodeled for the MVP (as the registry has them now)?
6. May a later step approve a live download of the footprints and CAMA layers (on the owner's machine or with the hosts allowed) to measure join rates?

## Live checks still needed [verify]

- Open the current dataset pages (footprints 2023 or newer; CAMA Residential, Commercial, Condominium; Use Codes; UBID; Common Ownership Lots) and record fields, row counts, last-updated and licence text.
- Measure: share of footprints with an SSL, share with STORIES, USECODE distribution, condo aggregation, tax-exempt coverage.
- Confirm Microsoft's licence (CDLA-P 2.0 vs ODbL) and the latest Overture release.
- Check DC GIS REST service terms and rate limits.

## Sources (accessed 2026-11-11 unless noted)

- Building Footprints 2023 catalog: https://catalog.data.gov/dataset/building-footprints-2023 (search summary; page not opened)
- Open Data DC landing page named by the catalog: https://opendata.dc.gov/datasets/DCGIS::building-footprints-2023 (not opened)
- Building Footprints 2021 schema mirror: https://baselight.app/u/usgov/dataset/district_of_columbia_building_footprints_2021 (search summary)
- DC Unique Building Identifier mirror: https://baselight.app/u/usgov/dataset/district_of_columbia_unique_building_identifier (search summary)
- CAMA Residential: https://catalog.data.gov/dataset/computer-assisted-mass-appraisal-residential-0dca4 and https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land/MapServer/25 (search summaries)
- CAMA Condominium: https://catalog.data.gov/dataset/computer-assisted-mass-appraisal-condominium-37d83 ; Commercial: https://catalog.data.gov/dataset/computer-assisted-mass-appraisal-commercial-48e20
- Property use codes: https://catalog.data.gov/dataset/integrated-tax-system-property-use-codes ; https://otr.cfo.dc.gov/sites/default/files/dc/sites/otr/publication/attachments/usecodes.pdf (not opened)
- DC terms of use (CC0 wording): https://beta.dc.gov/terms-and-conditions-use-district-data (search summary)
- Microsoft: https://github.com/microsoft/globalmlbuildingfootprints (read directly); https://planetarycomputer.microsoft.com/api/stac/v1/collections/ms-buildings (search summary)
- Overture: https://docs.overturemaps.org/guides/buildings , https://docs.overturemaps.org/schema/reference/buildings/building/ , https://docs.overturemaps.org/release-notes/ , https://overturemaps.org/about/faq/ (search summaries; pages not opened)
- OSM: https://wiki.openstreetmap.org/wiki/DC_Buildings_%26_Addresses_Import ; https://osmfoundation.org/wiki/Licence_and_Legal_FAQ ; https://osmfoundation.org/wiki/Attribution (search summaries)
