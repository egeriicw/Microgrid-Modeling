# ResStock upgrade measures (owner-supplied reference)

Supplied by the owner on 2026-11-06 as the list of ResStock measures/packages that the datasets simulate besides the
baseline. All are listed with initial dataset release **ResStock 2025 Release 1**. Measure documentation links were not
included in the paste and should be added from the official ResStock documentation.

> **Unverified assumptions (agent must confirm against the release's data dictionary and technical documentation):**
> 1. That "measure number" equals the `upgrade=N` partition id in the downloaded data (the owner's data uses
>    `upgrade=0` for the baseline).
> 2. Which release/weather version (TMY3 or AMY2018) the owner's local DC data comes from.
> 3. Whether later releases renumber or add upgrades (an NREL release note found during research mentioned upgrades
>    0–28 for one release, which does not match 32 measures here; so numbering is release-specific).

| # | Measure | Scenario family (proposed grouping) |
|---|---|---|
| 1 | Natural Gas Furnace 95% AFUE | Heating efficiency |
| 2 | Propane Furnace 95% AFUE and Fuel Oil Furnace 88% AFUE | Heating efficiency |
| 3 | Reference Space Heating and Air Conditioning Upgrade Circa 2025 | HVAC reference |
| 4 | Cold Climate Air-Source Heat Pump | Heat pump |
| 5 | Dual Fuel Heat Pump | Heat pump |
| 6 | Residential Single-Stage Geothermal Heat Pump (3.8 COP, 18.6 EER) | Geothermal heat pump |
| 7 | Residential Two-Stage Geothermal Heat Pump (4.0 COP, 20.5 EER) | Geothermal heat pump |
| 8 | Residential Variable-Speed Geothermal Heat Pump (4.4 COP, 30.9 EER) | Geothermal heat pump |
| 9 | Heat Pump Water Heater | Water heating |
| 10 | Natural Gas Water Heater | Water heating |
| 11 | Advanced Air Sealing* | Envelope |
| 12 | Attic Floor Insulation | Envelope |
| 13 | Duct Sealing and Insulation | Envelope |
| 14 | Advanced Air Sealing with Drill-and-Fill Wall Insulation | Envelope |
| 15 | Advanced Air Sealing, Attic Floor Insulation, and Duct Sealing and Insulation | Envelope package |
| 16 | Advanced Air Sealing, Drill-and-Fill Wall Insulation, Attic Floor Insulation, and Duct Sealing and Insulation | Envelope package |
| 17 | EnergyStar Windows | Envelope |
| 18 | Residential Two-Stage Geothermal Heat Pump (4.0 COP, 20.5 EER) With Envelope Improvements and Advanced Air Sealing | Geothermal + envelope package |
| 19 | Electric Vehicle Adoption with Level 1 Charging | EV |
| 20 | Electric Vehicle Adoption with Level 2 Charging | EV |
| 21 | Efficient Electric Vehicle Adoption with Level 2 Charging | EV |
| 22 | Electric Vehicle Adoption with Level 2 Charging and Demand Flexibility | EV + flexibility |
| 23 | Efficient Electric Vehicle Adoption with Level 2 Charging and Demand Flexibility | EV + flexibility |
| 24 | HVAC Demand Flexibility with On-Peak Load Shedding of 2F offset | Demand flexibility |
| 25 | HVAC Demand Flexibility with 4 hour Pre-peak Load Shifting of 2F offset | Demand flexibility |
| 26 | HVAC Demand Flexibility with On-Peak Load Shedding of 4F offset | Demand flexibility |
| 27 | HVAC Demand Flexibility with 4 hour Pre-peak Load Shifting of 4F offset | Demand flexibility |
| 28 | HVAC Demand Flexibility with 1 hour Pre-peak Load Shifting of 2F offset | Demand flexibility |
| 29 | Light Air Sealing | Envelope |
| 30 | Light Air Sealing, Attic Floor Insulation, and Duct Sealing and Insulation | Envelope package |
| 31 | Residential Two-Stage Geothermal Heat Pump (4.0 COP, 20.5 EER) With Light Envelope Improvements | Geothermal + envelope package |
| 32 | Residential Variable-Speed Geothermal Heat Pump (4.4 COP, 30.9 EER) with Light Envelope Improvements | Geothermal + envelope package |

## What this implies for the product
- The scenario catalog (FR-DATA-8) needs a **family** label and a human-readable name per upgrade, so users pick
  "Cold climate heat pump" or "EV with Level 2 charging" rather than a number.
- The scenarios the owner named map to families: **heat pumps** (4, 5, geothermal 6–8, 18, 31, 32), **EV charging**
  (19–23), plus envelope, water heating and demand flexibility.
- Demand-flexibility upgrades (24–28, 22, 23) change load *shape* (shedding/shifting) and are relevant to battery and
  virtual-power-plant analysis later.
- Many packages are **combinations**; the scenario rule (FR-ENG-12) must not double count when a building could appear
  in several upgrades.
- A ComStock equivalent list has not been supplied yet.
