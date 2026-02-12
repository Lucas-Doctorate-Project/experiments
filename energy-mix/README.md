# Energy Mix Data Collection

This document describes the rationale for selecting countries and the temporal scope of historical energy generation records (by production type) from different countries.

## How to Run the Script

Basically, we're just using `requests` and `pandas` libraries. You can install the version of the packages we used by running `pip install -r requirements.txt`.

## Country Selection

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

## Temporal Scope

The records were collected from the European transparency platform. Data is recorded every 15 minutes. Seven-day periods (one week) were selected to capture daily variations in the energy mix and variability between weekdays (high industrial and environmental load) and weekends (load relief). The selected period spans from January 11 to January 17 (European winter). The choice is due to operational stress.