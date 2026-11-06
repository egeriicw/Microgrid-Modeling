# ComStock upgrade measures (owner-supplied reference)

Supplied by the owner on 2026-11-06. Measure IDs are **strings** (e.g. `hvac_0005`), unlike ResStock's numbers.
"Initial release" is the first ComStock dataset release containing the measure. Documentation links were not included
in the paste. The `+` and `**` marks on some names are footnote markers from the source table whose meaning was not
supplied; they are preserved as given.

> **Unverified assumptions (an agent must confirm in each release's documentation and data dictionary):**
> 1. How a measure ID (`hvac_0005`) maps to the `upgrade=N` partition number in the downloaded data.
> 2. Which release and weather version the owner's local DC data comes from.
> 3. Whether every measure is available for DC and for every release (measures were added over time; a given release
>    contains only the measures that existed then).

| Family (proposed) | Measure ID | Measure name | Initial release |
|---|---|---|---|
| Envelope | env_0001 | Exterior Wall Insulation | 2023 R1 |
| Envelope | env_0002 | Roof Insulation | 2023 R1 |
| Envelope | env_0003 | Secondary Window System | 2023 R1 |
| Envelope | env_0004 | Window Film | 2023 R1 |
| Envelope | env_0005 | Window Replacement | 2023 R1 |
| Envelope | env_0006 | Upgrade Envelope to Latest Code | 2025 R2 |
| HVAC / heat pump | hvac_0001 | Air-Source Heat Pump Boiler | 2023 R1 |
| HVAC / heat pump | hvac_0002 | Air-Source Heat Pump Boiler and Natural Gas Boiler Backup | 2023 R2 |
| HVAC / heat pump | hvac_0003 | DOAS with Mini Split Heat Pumps | 2023 R1 |
| HVAC / heat pump | hvac_0004 | Heat Pump RTU with Original Fuel Backup | 2023 R2 |
| HVAC / heat pump | hvac_0005 | Heat Pump RTU | 2023 R1 |
| HVAC / heat pump | hvac_0006 | VRF Heat Recovery with DOAS | 2023 R2 |
| HVAC efficiency | hvac_0007 | Demand Control Ventilation | 2023 R2 |
| HVAC efficiency | hvac_0008 | Exhaust Air Heat/Energy Recovery | 2023 R2 |
| HVAC efficiency | hvac_0009 | Economizers | 2024 R1 |
| HVAC / heat pump | hvac_0010 | Variable Refrigerant Flow With 25% Upsizing Allowance for Heating | 2024 R1 |
| HVAC / heat pump | hvac_0011 | Standard Performance Heat Pump Rooftop Unit with New Windows | 2025 R1 |
| HVAC efficiency | hvac_0012 | Improved Fan Scheduling and Control of Outdoor Air During Unoccupied Periods | 2024 R1 |
| HVAC / heat pump | hvac_0013 | Heat Pump RTU with Exhaust Air Energy Recovery | 2024 R1 |
| HVAC efficiency | hvac_0014 | Advanced Rooftop Unit Control | 2024 R1 |
| Geothermal | hvac_0015 | Central Ground-Source Water-to-Water Heat Pump | 2024 R1 |
| Geothermal | hvac_0016 | Packaged Water-to-Air Geothermal Heat Pump | 2024 R1 |
| Geothermal | hvac_0017 | Ground-Coupled Console Water-to-Air Heat Pump | 2024 R1 |
| HVAC / heat pump | hvac_0018 | Heat Pump Rooftop Units With Standard Performance | 2024 R2 |
| HVAC / heat pump | hvac_0019 | Standard Performance Heat Pump Rooftop Unit with New Roof | 2024 R2 |
| HVAC / heat pump | hvac_0020 | Heat Pump Rooftop Units With Higher Compressor Lockout Temperature for Heating | 2024 R2 |
| HVAC / heat pump | hvac_0021 | Heat Pump Rooftop Units Compliant With the Commercial Building Heat Pump Technology Challenge Specification** | 2024 R2 |
| HVAC (modeling) | hvac_0022 | Ideal Thermal Air Loads | 2024 R2 |
| HVAC / heat pump | hvac_0023 | Reduced Thermostat Setbacks for Heat Pumps | 2025 R1 |
| HVAC efficiency | hvac_0024 | Chiller Replacement | 2025 R1 |
| HVAC / heat pump | hvac_0025 | Laboratory-Informed Modeling of Standard Performance Heat Pump Rooftop Units | 2025 R1 |
| HVAC efficiency | hvac_0026 | Condensing Gas Boilers | 2025 R1 |
| HVAC / electrification | hvac_0027 | Electric Resistance Boilers | 2025 R1 |
| HVAC efficiency | hvac_0028 | High-Efficiency Rooftop Unit | 2025 R3 |
| HVAC efficiency | hvac_0029 | Variable Speed Pumps | 2025 R3 |
| HVAC efficiency | hvac_0030 | Thermostat Setbacks During Unoccupied Periods | 2025 R3 |
| HVAC efficiency | hvac_0031 | Fan Static Pressure Reset for Multizone VAV Systems | 2025 R3 |
| Lighting | ltg_0001 | LED Lighting | 2023 R1 |
| Lighting | ltg_0002 | Lighting Controls | 2025 R2 |
| Plug / process | ppl_0001 | Electric Cooking Equipment | 2024 R1 |
| Generation (PV / storage) | gen_0001 | Photovoltaics With 40% Rooftop Coverage | 2025 R1 |
| Generation (PV / storage) | gen_0002 | Photovoltaics With 40% Rooftop Coverage and Battery Storage | 2025 R2 |
| Demand response | dr_0001 | Thermostat Control for Load Shedding | 2024 R1 |
| Demand response | dr_0002 | Thermostat Control for Load Shifting | 2024 R1 |
| Demand response | dr_0003 | Lighting Control for Load Shedding | 2024 R2 |
| Demand response | dr_0004 | Lighting Control for Emission Reduction+ | 2024 R2 |
| Demand response | dr_0005 / dr_0006 | Thermostat and Lighting Control for Load Shedding | 2025 R1 |
| Demand response | dr_0007 | Low Priority Device Switching During Peak Hours+ | 2025 R1 |
| Demand response | dr_0008 | Dim Lighting During Peak Hours+ | 2025 R1 |
| Demand response | dr_0009 | Electric HVAC Temperature Setpoint Adjustment During Peak Hours** | 2025 R1 |
| Demand response | dr_0010 | Electric HVAC Preheating by Thermostat Control** | 2025 R1 |
| Demand response | dr_0011 | Electric HVAC Precooling by Thermostat Control** | 2025 R1 |
| Package | pkg_0001 | Wall and Roof Insulation and New Windows | 2023 R2 |
| Package | pkg_0002 | LED Lighting, HP-RTU and ASHP-Boiler | 2023 R2 |
| Package | pkg_0003 | Wall and Roof Insulation, New Windows, LED Lighting, HP-RTU and ASHP-Boiler | 2023 R2 |
| Package | pkg_0004 | LED Lighting, Standard Performance HP-RTU and ASHP-Boiler | 2024 R2 |
| Package | pkg_0005 | HP-RTU + ASHP Boiler + Demand Control Ventilation + Heat/Energy Recovery + Economizers | 2024 R1 |
| Package (geothermal) | pkg_0006 | Comprehensive Geothermal Heat Pump Package, Hydronic GHP, Packaged GHP, or Console GHP | 2024 R1 |
| Package (DR) | pkg_0007 | Thermostat and Lighting Control for Load Shedding | 2025 R1 |
| Package (DR + PV) | pkg_0008 | Thermostat and Lighting Control for Load Shedding + Photovoltaics With 40% Rooftop Coverage | 2025 R1 |
| Package (DR) | pkg_0009 | Demand Flexibility, Lighting + Thermostat Control, Load Shed for Daily Bldg Peak Reduction | 2024 R2 |
| Package (geothermal) | pkg_0010 | Geothermal Heat Pumps + High-Efficiency Envelope Package | 2025 R1 |
| Package (geothermal) | pkg_0011 | Geothermal Heat Pumps + High-Efficiency Envelope + LED Lighting Package | 2025 R1 |
| Package (PV) | pkg_0012 | Photovoltaics With 40% Rooftop Coverage With Roof Insulation+ | 2025 R2 |

## What this implies for the product
- Scenario ids differ by product (ResStock numeric, ComStock string), so the catalog (FR-DATA-8) needs a stable internal
  scenario key per (product, release) plus the partition id used on disk.
- **Availability depends on release.** The scenario picker must only offer measures present in the release actually held.
- **Generation scenarios exist.** `gen_0001`, `gen_0002` (PV with and without battery) and `pkg_0008`, `pkg_0012` bundle
  on-site PV/storage into the building load. Those results are *net* of generation, not gross demand. The engine must
  record whether a dataset's load is gross or net, and must never mix them silently in one aggregate. This is a
  methodology and labeling decision for the owner.
- Demand-response measures change shape and timing (shed/shift/precool) and matter for battery and VPP analysis later.
- ResStock has no equivalent PV measure in the supplied list; any residential PV scenario would need another source.
