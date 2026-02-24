# Energy Data

This folder contains two types of energy-related reference data used in the experiments:

1. **Energy mix traces** (`*_trace.csv`) — one-week time-series of energy generation by source, consumed by Batsim's `--environmental-footprint-dynamic` option.
2. **Intensity factors** (`intensities.json`) — lookup table of carbon and water intensity per energy technology.

---

## Energy Mix Traces

### Country Selection

The countries chosen were France, Germany, and Poland. The reason behind these choices is the difference in their energy mixes. More info can be found in the following links:

- https://en.wikipedia.org/wiki/Energy_in_France
- https://en.wikipedia.org/wiki/Energy_in_Germany
- https://en.wikipedia.org/wiki/Energy_in_Poland

| Country | Energy Mix Profile | Rationale |
| :--- | :--- | :--- |
| France | Stability and low carbon | Will be used to model the green-dominated scenario. Strong presence of green sources: solar, hydro, wind, and nuclear. Energy mix dominated by nuclear power. |
| Germany | Transition and volatility | Will be used to model the mixed scenario. During the day, considerable presence of intermittent renewable sources (wind and solar). During the night, there is a slight tendency toward fossil fuel burning. |
| Poland | Fossil-dominated | Will be used to model the fossil-dominated scenario (fossil fuel burning). Green contribution is small. |

The files corresponding to the extracted data for each country are `fossil_heavy_trace.csv` (Poland), `clean_energy_trace.csv` (France), and `mixed_trace.csv` (Germany).

### Temporal Scope

The records were collected from the European transparency platform. Data is recorded every 15 minutes. Seven-day periods (one week) were selected to capture daily variations in the energy mix and variability between weekdays (high industrial and environmental load) and weekends (load relief). The selected period spans from January 11 to January 17 (European winter). The choice is due to operational stress.

### How to Regenerate the Traces

Install the required packages and run `scrap_energy_mix.py`:

```bash
pip install -r requirements.txt
python scrap_energy_mix.py
```

### Observation

We only update `master_host` since it will be the only host responsible for tracking the energy mix. It has to be the first host in the platform file (design choices).

---

## Intensity Factors (`intensities.json`)

Lookup table of intensity factors for water consumption and carbon emissions from energy generation technologies. The water factors are from Macknick et al. 2012, while carbon factors are from IPCC 2014 and UNECE 2020 reports. The units are L/kWh (liters per kilowatt-hour) for water and gCO2eq/kWh (grams of CO2 equivalent per kilowatt-hour) for carbon. Water values were converted from gallons/MWh to L/kWh.

### Technology Lookup

Use composite keys to look up specific technologies:
- Water: `fuel-technology-subtype-cooling-ccs`
  - Examples: `coal-pc-subc-tower`, `gas-ngcc-dry`, `solar-pv`
- Carbon: `fuel-technology-subtype-cooling-ccs-source`
  - Examples: `coal-pc-ipcc-2014`, `gas-ngcc-unece-2020`, `solar-pv-utility-ipcc-2014`
  - Sources: `ipcc-2014` or `unece-2020`

Use defaults when technology details are unknown:

- Water: `coal-default`, `gas-default`, `nuclear-default`, `csp-default`, `biopower-default`, `geothermal-default`, `hydro-default`, `solar-pv-default`, `wind-default`
- Carbon: `coal-default-ipcc-2014`, `gas-default-unece-2020`, `solar-pv-default-ipcc-2014`, etc.

### Key Naming Convention

Uses standard IPCC/UNECE abbreviations:

- **fuel**: `coal`, `gas`, `nuclear`, `solar-pv`, `wind`, `hydro`, `csp`, `biopower`, `geothermal`, `biomass`, `ocean`
- **technology**:
  - `pc` (Pulverized Coal)
  - `igcc` (Integrated Gasification Combined Cycle)
  - `ngcc` (Natural Gas Combined Cycle)
  - `steam`, `biogas`, `flash`, `binary`, `egs`, etc.
- **subtype**:
  - `subc` (Subcritical)
  - `sc` (Supercritical)
  - `usc` (Ultra-Supercritical)
  - `trough`, `power-tower`, `fresnel`, `stirling`, etc.
- **cooling**: `tower`, `once-through`, `pond`, `dry`, `hybrid`
- **ccs**: included in key when carbon capture and storage is present

Not all technologies have all attributes. Keys are constructed from available information.

### References

- Macknick, J., et al. (2012). "Operational water consumption and withdrawal factors for electricity generating technologies: a review of existing literature." Environmental Research Letters 7(4): 045802.
- Wiki with the carbon intensity. https://en.wikipedia.org/wiki/Life-cycle_greenhouse_gas_emissions_of_energy_sources.
- IPCC (2014). Climate Change 2014: Mitigation of Climate Change. Working Group III Contribution to the Fifth Assessment Report.
- UNECE (2020). Life Cycle Assessment of Electricity Generation Options. United Nations Economic Commission for Europe.
