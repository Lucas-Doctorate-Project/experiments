# Experimental Setup

This repository contains most files used for our experiments.

- The [`energy-mix/`](energy-mix/) folder contains the one week energy mix traces in the format supported by our SimGrid/Batsim plugin.
- The [`intensity-factors/`](intensity-factors/) folder contains all the intensity factors that we've collected in existing literature. The intensities are stored in a JSON enabling easy queries.
- The [`workloads/`](workloads/) folder contains the Batsim-compatible workloads.

# To Do

- [ ] Add the platform file.
- [X] Add the workload files.
- [X] Add the intensity factors as a JSON file that can be easily fetched.
- [ ] Create the script to run the experiments.