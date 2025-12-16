# MBRP_acc – Accelerometry processing and sleep classification

This repository contains a pipeline for processing tri-axial accelerometry (ACC) exports (e.g., from Movebank), 
deriving VeDBA, and classifying sleep/awake bouts, with basic plotting and visualization utilities. T
he code is a mix of R, Python, and Jupyter notebooks, organized under the `code/` folder.

## Contents

Core scripts in `code/`:

- Movebank ACC wrangling
  - 00_get_acc_from_movebank_format_long.py: Convert Movebank ACC export into a long format suitable for downstream processing.
  - write_long_acc.R: R alternative for writing ACC to long format.
  - write_long_acc_lilac_continous.R: Variant for continuous ACC streams (lilac).

- VeDBA derivation
  - 01a_acc_to_vedba_par.py: Compute VeDBA from ACC, with parallelization (Python).
  - 01b_get_vedba_from_acc.R: Compute VeDBA from ACC (R implementation).

- Sleep classification
  - 02_sleep_classification_alogrithm.R: Classify sleep/awake states from VeDBA time series.
  - sleep_classification_alogrithm_parallel.R: Parallelized variant of the sleep classification.

- Exploration and visualization
  - plot_basic_vedba.ipynb: Quick plots and checks for VeDBA.
  - vis_sleep_trajectories.Rmd: Visualize individual and group sleep trajectories.
  - find_major_awake_events_trail.R: Identify major awake events/bouts.

- Metadata
  - get_movebank_metadata.R: Fetch/prepare relevant metadata from Movebank.


## Typical workflow

Export ACC (and optionally metadata) from Movebank.
Tidy ACC into long format:
   - Python: `00_get_acc_from_movebank_format_long.py`
   - or R: `write_long_acc.R` or `write_long_acc_lilac_continous.R` (for continuous streams)
Derive VeDBA:
   - Python: `01a_acc_to_vedba_par.py` (parallelized)
   - or R: `01b_get_vedba_from_acc.R`
Classify sleep/awake states:
   - Serial: `02_sleep_classification_alogrithm.R`
   - Parallel: `sleep_classification_alogrithm_parallel.R`
