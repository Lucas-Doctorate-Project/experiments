# Energy Intensity Factors

Lookup table of intensity factors for water consumption and carbon emissions from energy generation technologies. The water factors are from Macknick et al. 2012, while carbon factors are from IPCC 2014 and UNECE 2020 reports. The units are L/kWh (liters per kilowatt-hour) for water and gCO2eq/kWh (grams of CO2 equivalent per kilowatt-hour) for carbon. Water values were converted from gallons/MWh to L/kWh.

## Technology Lookup

Use composite keys to look up specific technologies:
- Water: `fuel-technology-subtype-cooling-ccs`
  - Examples: `coal-pc-subc-tower`, `gas-ngcc-dry`, `solar-pv`
- Carbon: `fuel-technology-subtype-cooling-ccs-source`
  - Examples: `coal-pc-ipcc-2014`, `gas-ngcc-unece-2020`, `solar-pv-utility-ipcc-2014`
  - Sources: `ipcc-2014` or `unece-2020`

Use defaults when technology details are unknown:

- Water: `coal-default`, `gas-default`, `nuclear-default`, `csp-default`, `biopower-default`, `geothermal-default`, `hydro-default`, `solar-pv-default`, `wind-default`
- Carbon: `coal-default-ipcc-2014`, `gas-default-unece-2020`, `solar-pv-default-ipcc-2014`, etc.

## Key Naming Convention

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

## References

- Macknick, J., et al. (2012). "Operational water consumption and withdrawal factors for electricity generating technologies: a review of existing literature." Environmental Research Letters 7(4): 045802.
- Wiki with the carbon intensity. https://en.wikipedia.org/wiki/Life-cycle_greenhouse_gas_emissions_of_energy_sources.
- IPCC (2014). Climate Change 2014: Mitigation of Climate Change. Working Group III Contribution to the Fifth Assessment Report.
- UNECE (2020). Life Cycle Assessment of Electricity Generation Options. United Nations Economic Commission for Europe.
