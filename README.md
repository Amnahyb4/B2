# B2 — Data ETL & EDA Pipeline

## 📌 Description

**B2** is a Python-based data engineering project that implements a complete **ETL (Extract, Transform, Load)** workflow followed by **Exploratory Data Analysis (EDA)**.

The project:
- Loads raw CSV data
- Cleans and transforms datasets
- Builds analytics-ready Parquet tables
- Generates summary statistics and visualizations
- Saves outputs in a structured and reproducible way

The pipeline is modular, easy to run, and designed for clarity and reproducibility.

---

## ✨ Features

- Structured ETL pipeline (load → clean → analytics)
- Parquet-based storage for efficient analytics
- Modular and reusable Python code
- Automated metadata logging
- Exploratory Data Analysis with visualizations
- Fast dependency management using **uv**

---

## 🛠️ Setup (Using `uv`)

This project uses **uv** for dependency management and virtual environments.

## Setup (using uv)

### 1. Install uv (if not already installed)
bash
pip install uv

### 2. Create a virtual environment
bash
uv venv


### 3. Activate the virtual environment
bash
source .venv/bin/activate   # macOS / Linux
.venv\Scripts\activate    # Windows


## 4. Install dependencies
bash
uv pip install -r requirements.txt

---

## How to run the ETL

From the project root directory, run:
bash
python scripts/run_etl.py


## How to run the EDA notebook

### 1. Start Jupyter:
bash
jupyter notebook


### 2. Open:
bash
notebooks/eda.ipynb


### 3. Run all cells to reproduce the exploratory analysis using the processed data in data/processed/.


This will:
- Load raw CSV files from data/raw/
- Clean and transform the data
- Join orders with users
- Generate analytics features and flags
- Write processed outputs and run metadata


---

## 📦 Outputs

After a successful ETL run, the following files are generated:

### Processed Data
- `data/processed/orders.parquet`
- `data/processed/orders_clean.parquet`
- `data/processed/users.parquet`
- `data/processed/users_clean.parquet`
- `data/processed/analytics_table.parquet`

### Metadata
- `data/processed/_run_meta.json`

### Visualizations
- `reports/figures/*.png`


## 📂 Project Structure

```text
B2/
├── data/
│   ├── raw/                # Raw CSV input files
│   └── processed/          # Cleaned & analytics Parquet files
│
├── scripts/                # ETL & utility scripts
│   ├── run_day1_load.py
│   ├── run_day2_clean.py
│   ├── run_day3_build_analytics.py
│   ├── run_etl.py          # Run full ETL pipeline
│   └── view_all_parquet.py # Inspect analytics table
│
├── notebooks/
│   └── eda.ipynb           # Exploratory Data Analysis
│
├── reports/
│   └── figures/            # Generated plots (.png)
│
├── src/
│   └── data_workflow/      # Reusable pipeline modules
│
├── pyproject.toml          # Project dependencies (uv)
├── README.md               # Project documentation
└── .venv/                  # Virtual environment (not committed) '''

---

