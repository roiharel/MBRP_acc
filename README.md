# MBRP_acc – Accelerometry processing and sleep classification

This repository contains a pipeline for processing tri-axial accelerometry (ACC) data from Movebank, deriving VeDBA (Vectorial Dynamic Body Acceleration), and classifying sleep/awake states. The code is a mix of R, Python, and Jupyter notebooks, organized under the `code/` folder.

## Contents

### Core Pipeline Scripts (in `code/`)

The numbered scripts form the main processing pipeline:

| Script | Language | Description |
|--------|----------|-------------|
| `00_get_acc_data_from_movebank.py` | Python | Download and merge accelerometry data from Movebank API, saves as parquet |
| `01_prep_tag_acc_long.py` | Python | Prepare tag ACC data in long format, apply calibration, per-animal processing |
| `02_acc_to_vedba_par.py` | Python | Compute VeDBA from ACC bursts with parallelization |
| `03_find_inactivity.R` | R | Classify sleep/awake states using Gaussian mixture models on log VeDBA |
| `04_get_sleep_metrics.R` | R | Extract sleep periods, onset/waking times, sleep efficiency metrics |

### Additional Analysis Scripts

| Script | Language | Description |
|--------|----------|-------------|
| `find_major_awake_events.R` | R | Detect group-level awake events using proportional threshold method |

### Exploration and Visualization

| Script | Language | Description |
|--------|----------|-------------|
| `plot_basic_vedba.ipynb` | Jupyter | Quick plots and checks for VeDBA data |
| `vis_sleep_trajectories.Rmd` | R Markdown | Visualize individual and group sleep trajectories |

### Helper Functions (in `code/functions/`)

| Script | Language | Description |
|--------|----------|-------------|
| `fetch_movebank_data.py` | Python | `MovebankDataFetcher` class for API access (requires credentials) |
| `get_sleep_sites_used.R` | R | Identify sleeping sites using DBSCAN clustering on GPS data |

### Automation

| Script | Description |
|--------|-------------|
| `run_acc2vedbarun_process_sleep_metrics_tmux.sh` | Bash script to run pipeline steps 00-02 in a tmux session |

## Typical Workflow

1. **Download ACC data from Movebank**
   ```bash
   python code/00_get_acc_data_from_movebank.py
   ```

2. **Prepare ACC data in long format with calibration**
   ```bash
   python code/01_prep_tag_acc_long.py
   ```

3. **Compute VeDBA (parallelized)**
   ```bash
   python code/02_acc_to_vedba_par.py
   ```

4. **Classify sleep/awake states**
   ```r
   source("code/03_sleep_classification_alogrithm.R")
   ```

5. **Extract sleep period metrics**
   ```r
   source("code/04_find_sleep_periods.R")
   ```

**Alternatively**, use the tmux automation script:
```bash
./code/run_acc2vedba_tmux.sh start
```

## Requirements

### Python
- pandas, numpy, pyarrow, tqdm
- pyreadr (for R data files)
- requests, python-dotenv (for Movebank API)

### R
- data.table, dplyr, tidyr, lubridate
- arrow, zoo, suncalc
- mixtools (for Gaussian mixture models)
- move2 (for Movebank integration)
- sf, dbscan (for spatial clustering)
