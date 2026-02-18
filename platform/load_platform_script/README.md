# Platform Generator

It generates a SimGrid platform based on three predefined
energy profiles --- **clean**, **mixed**, or **fossil** --- combined
with lifecycle carbon and water intensity data provided in an external
`intensities.json` file.

The file `platform.cpp` builds a shared library (`libplatform.so`) that
can be loaded using `Engine::load_platform()`, as described in the
SimGrid documentation:

https://simgrid.org/doc/latest/Platform_cpp.html#loading-the-platform

The file `example.cpp` demonstrates how to load and use the generated
platform.

------------------------------------------------------------------------

## Environment Variables

Two environment variables control the platform generation:

-   **PLAT_TYPE**\
    Defines the energy mix profile.\
    Accepted values:
    -   clean
    -   mixed
    -   fossil
-   **PLAT_JSON**\
    Path to the JSON file containing lifecycle intensity data.

------------------------------------------------------------------------

## Build

    mkdir -p build
    cmake -S . -B build
    cmake --build build -j

This produces: - `libplatform.so` - `example`

------------------------------------------------------------------------

## Run

Example:

    PLAT_TYPE=mixed PLAT_JSON=../../intensity-factors/intensities.json ./example

------------------------------------------------------------------------

## Unit Conversion

The JSON file provides water intensity in gal/MWh.

Internally, values are converted to:

L/kWh = gal/MWh × 0.00378541

This ensures compatibility with the environmental footprint plugin documentation.

------------------------------------------------------------------------

# Methodological Considerations and Assumptions

The selected intensity values and technology weightings are based on the
following considerations:

## Carbon Capture and Storage (CCS)

CCS-equipped plants appear to be rare in the European Union, with only
limited deployment.

Sources: -
https://www.catf.us/2025/11/carbon-capture-storage-europe-slow-but-significant-progress-2025/ -
https://ccusia.ccsknowledge.com/insight-accelerator/international-ccs-projects/

As a result, CCS variants are not assumed to represent dominant
generation shares.

## Cooling Technologies

Dry and hybrid cooling systems are significantly less common than
once-through or recirculating systems.

According to U.S. EIA data (2012): - 719 once-through systems - 819
recirculating systems - 61 dry/hybrid systems

In the absence of equivalent EU data, and assuming similar technology
maturity levels across developed economies, dry/hybrid cooling is
assumed to account for less than 4% of installed cooling systems in EU
thermal plants.

Source:
https://climate-adapt.eea.europa.eu/en/metadata/adaptation-options/reducing-water-consumption-for-cooling-of-thermal-generation-plants

## Fossil Hard Coal

Pulverized Coal (PC) technology is the dominant coal generation
technology worldwide.

Subcritical PC plants --- the most common configuration --- typically
operate at around 35% efficiency.

Source: UNECE 2020, Section 3.1.1

## Hydropower

For lifecycle emissions:

-   The 660 MW hydropower plant (hydro-large) is considered an outlier
    due to unusually long transportation distances during construction.
-   The 360 MW plant (hydro-medium) is considered more representative.

Therefore, hydro-medium values are weighted more heavily in the
aggregated intensity.

Source: UNECE 2020, Section 4.1.1

## Fossil Coal-Derived Gas

This category is assumed to correspond to electricity produced via
Integrated Gasification Combined Cycle (IGCC) technology.

## Fossil Brown coal/Lignite 

>Notes: 
    - found this: https://www.volker-quaschning.de/datserv/CO2-spez/index_e.php (one of the "References" in UNECE 2020)
    - 1073 g CO2‑eq/kWh (mean) - DOES NOT INCLUDE THE FULL LIFECYCLE ASSESSMENT (LCA) 

## Solar Energy (PV vs CSP)

### Photovoltaics (PV)

"Historically, **crystalline silicon PV has been the technology of choice globally**, with polycrystalline silicon cells representing the main market share of manufactured PV until 2015. Polycrystalline silicon panels are made of pieces of crystallized silicon melted together, which makes them relatively inexpensive to manufacture, but also less efficient, than their single-crystal counterpart, or monocrystalline silicon panels. The latter has tended to dominate the recent market." 

Source: UNECE 2020, Section 3.4.1

### Concentrated Solar Power (CSP)

CSP remains a niche market relative to PV, with approximately 6.5 GW
installed globally as of 2020.

Source: UNECE 2020, Section 3.5

Accordingly, PV is treated as the representative solar technology.

## Pumped Storage

Pumped storage is excluded from lifecycle emission accounting because it
is not an energy source. Its environmental impact depends entirely on
the electricity used to pump the water.

Source: UNECE 2020, Section 3.6.1

