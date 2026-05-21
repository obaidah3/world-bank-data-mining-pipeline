# End-to-End Web Scraping & Data Mining Pipeline
### ETL · Frequent Pattern Mining · Clustering · Classification

> **World Bank Open Data API · 7 Countries · 6 Macroeconomic Indicators · 2000–2023**

---

## Abstract

This project implements a production-grade **Extract–Transform–Load (ETL) pipeline** targeting the World Bank Open Data API — a publicly accessible, richly structured source that provides standardised macroeconomic indicators for every country worldwide. Because the API delivers raw JSON rather than pre-processed tables, it mirrors the challenges of real-world web scraping: heterogeneous missing-value patterns, unit inconsistencies, and multi-dimensional data shapes that must be pivoted before analysis.

The pipeline proceeds through five sequential stages: **(1)** parallel HTTP extraction with retry logic and pagination, **(2)** per-country outlier winsorisation and gap-fill interpolation, **(3)** dual-format persistence to CSV and SQLite, **(4)** unsupervised and supervised data mining, and **(5)** a consolidated reporting dashboard. The result is a fully reproducible, end-to-end research artifact that transforms raw API responses into actionable economic intelligence.

---

## Table of Contents

1. [Research Motivation](#1-research-motivation)
2. [Data Source](#2-data-source)
3. [Pipeline Architecture](#3-pipeline-architecture)
4. [Project Structure](#4-project-structure)
5. [Dependencies](#5-dependencies)
6. [Configuration](#6-configuration)
7. [Stage I — Data Extraction](#7-stage-i--data-extraction)
8. [Stage II — Data Transformation](#8-stage-ii--data-transformation)
9. [Stage III — Data Loading](#9-stage-iii--data-loading)
10. [Stage IV — Feature Engineering](#10-stage-iv--feature-engineering)
11. [Stage V — Data Mining](#11-stage-v--data-mining)
12. [Results & Outputs](#12-results--outputs)
13. [Reproducibility](#13-reproducibility)
14. [Limitations & Future Work](#14-limitations--future-work)
15. [References](#15-references)

---

## 1. Research Motivation

Understanding macroeconomic divergence across nations is a central challenge in development economics. While raw data is increasingly available through open APIs, the path from raw API responses to interpretable, machine-learning-ready datasets involves numerous non-trivial engineering decisions. This project addresses three interrelated research questions:

1. **Association:** What co-occurrence patterns exist between macroeconomic indicators across country-year observations?
2. **Structure:** Can country-year observations be partitioned into economically coherent clusters without supervision?
3. **Prediction:** Which features are most predictive of a country's GDP tier, and how accurately can a classifier recover that tier from observable indicators?

The pipeline is designed to answer all three questions within a single, reproducible notebook execution.

---

## 2. Data Source

| Property | Detail |
|---|---|
| **Provider** | World Bank Open Data |
| **Website** | https://data.worldbank.org |
| **API Base URL** | https://api.worldbank.org/v2 |
| **Access** | Public REST API — no authentication required |
| **Format** | JSON (paginated) |
| **Coverage** | 7 countries × 6 indicators × 24 years (2000–2023) |
| **Total Endpoints** | 42 (one per country × indicator pair) |

### Countries

| ISO-3 | Country | Website |
|---|---|---|
| EGY | Egypt | https://data.worldbank.org/country/EG |
| USA | United States | https://data.worldbank.org/country/US |
| FRA | France | https://data.worldbank.org/country/FR |
| DEU | Germany | https://data.worldbank.org/country/DE |
| CHN | China | https://data.worldbank.org/country/CN |
| BRA | Brazil | https://data.worldbank.org/country/BR |
| IND | India | https://data.worldbank.org/country/IN |

### Indicators

| Code | Description | Unit | Website |
|---|---|---|---|
| NY.GDP.PCAP.CD | GDP per Capita | Current USD | https://data.worldbank.org/indicator/NY.GDP.PCAP.CD |
| SL.UEM.TOTL.ZS | Unemployment Rate | % of labour force | https://data.worldbank.org/indicator/SL.UEM.TOTL.ZS |
| FP.CPI.TOTL.ZG | Inflation, CPI | Annual % | https://data.worldbank.org/indicator/FP.CPI.TOTL.ZG |
| SP.POP.TOTL | Total Population | Count | https://data.worldbank.org/indicator/SP.POP.TOTL |
| SP.DYN.LE00.IN | Life Expectancy at Birth | Years | https://data.worldbank.org/indicator/SP.DYN.LE00.IN |
| IT.NET.USER.ZS | Internet Users | % of population | https://data.worldbank.org/indicator/IT.NET.USER.ZS |

---

## 3. Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    WORLD BANK OPEN DATA API                         │
│              https://api.worldbank.org/v2                           │
└──────────────────────────┬──────────────────────────────────────────┘
                           │  42 parallel HTTP requests
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STAGE 1 · EXTRACT                                                  │
│  ThreadPoolExecutor · Retry + Backoff · Pagination · Provenance     │
└──────────────────────────┬──────────────────────────────────────────┘
                           │  Raw JSON → DataFrame (long format)
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STAGE 2 · TRANSFORM                                                │
│  Deduplication · Year Filter · Pivot · Interpolation                │
│  Forward/Back Fill · Per-Country Winsorisation · NaN Drop           │
└──────────────────────────┬──────────────────────────────────────────┘
                           │  Clean DataFrame (wide format)
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STAGE 3 · LOAD                                                     │
│  raw_worldbank.csv · clean_worldbank.csv · worldbank.db (SQLite)    │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STAGE 4 · FEATURE ENGINEERING                                      │
│  GDP_LOG · ECON_STABILITY · HUMAN_DEV · GROWTH_RATE                 │
│  HIGH_GDP (classification target) · StandardScaler                  │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STAGE 5 · DATA MINING                                              │
│                                                                     │
│  ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────┐   │
│  │ ASSOCIATION     │  │  CLUSTERING      │  │ CLASSIFICATION   │   │
│  │ Apriori         │  │  K-Means         │  │ Random Forest    │   │
│  │ FP-Growth       │  │  PCA + UMAP      │  │ Gradient Boost   │   │
│  │ Support/Conf/   │  │  Radar Profiles  │  │ Logistic Regr.   │   │
│  │ Lift/Conviction │  │  Silhouette      │  │ SVM + ROC/AUC    │   │
│  └─────────────────┘  └──────────────────┘  └──────────────────┘   │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STAGE 6 · REPORTING                                                │
│  10 Figures · Summary Dashboard · Pipeline ZIP Export               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 4. Project Structure

```
project/
│
├── WEB_Scraping_ETL_Professional_Enhanced.ipynb   ← Main notebook
│
├── pipeline_data/                                  ← Auto-generated outputs
│   ├── raw_worldbank.csv                          ← Raw extraction snapshot
│   ├── clean_worldbank.csv                        ← Cleaned & engineered data
│   ├── worldbank.db                               ← SQLite database
│   ├── best_model.joblib                          ← Serialised best classifier
│   ├── fig1_raw_profile.png
│   ├── fig2_before_after_cleaning.png
│   ├── fig3_correlation.png
│   ├── fig4_timeseries.png
│   ├── fig5_clustering_pca.png
│   ├── fig6_umap.png
│   ├── fig7_radar_profiles.png
│   ├── fig8_association_rules.png
│   ├── fig9_classification.png
│   ├── fig10_feature_importance.png
│   └── fig11_dashboard.png
│
└── worldbank_links.json                           ← All source URLs
```

---

## 5. Dependencies

```bash
pip install requests pandas numpy matplotlib seaborn scikit-learn \
            mlxtend tqdm joblib umap-learn
```

| Library | Version | Purpose |
|---|---|---|
| `requests` | ≥2.28 | HTTP client for World Bank API |
| `pandas` | ≥1.5 | DataFrame manipulation and pivoting |
| `numpy` | ≥1.23 | Numerical operations |
| `scikit-learn` | ≥1.2 | Clustering, classification, scaling |
| `mlxtend` | ≥0.21 | Apriori, FP-Growth, association rules |
| `matplotlib` / `seaborn` | ≥3.6 / ≥0.12 | Visualisation |
| `umap-learn` | ≥0.5 | Non-linear dimensionality reduction |
| `tqdm` | ≥4.64 | Progress bars |
| `joblib` | ≥1.2 | Model persistence |

> **Note:** `umap-learn` is optional. The pipeline degrades gracefully if not installed, skipping UMAP visualisation while all other stages execute normally.

---

## 6. Configuration

All hyperparameters are centralised in a single `PipelineConfig` dataclass, making the pipeline fully configurable without modifying any logic code.

```python
@dataclass
class PipelineConfig:
    base_url      : str        = "https://api.worldbank.org/v2"
    countries     : List[str]  = ["EGY","USA","FRA","DEU","CHN","BRA","IND"]
    start_year    : int        = 2000
    end_year      : int        = 2023
    fetch_workers : int        = 6       # Parallel HTTP threads
    min_support   : float      = 0.3     # Apriori/FP-Growth threshold
    min_confidence: float      = 0.6     # Association rule confidence
    n_clusters    : int        = 4       # K-Means clusters
    kmeans_n_init : int        = 20      # K-Means initialisations
    test_size     : float      = 0.25    # Train/test split ratio
    cv_folds      : int        = 5       # Stratified cross-validation folds
    random_state  : int        = 42      # Global reproducibility seed
```

---

## 7. Stage I — Data Extraction

### Architecture

The extractor implements a `WorldBankScraper` class backed by a persistent `requests.Session` with a custom connection pool sized to `fetch_workers`. All 42 country × indicator combinations are submitted simultaneously to a `ThreadPoolExecutor`, reducing wall-clock extraction time by up to 6× compared to sequential fetching.

### URL Pattern

```
https://api.worldbank.org/v2/country/{ISO3}/indicator/{CODE}
    ?format=json&per_page=100&date={start}:{end}&page={N}
```

### Reliability Features

| Feature | Implementation |
|---|---|
| **Retry logic** | Exponential back-off up to 4 attempts; handles HTTP 429 and 503 |
| **Pagination** | Walks all result pages by incrementing `page` parameter |
| **Rate limiting** | Randomised 0.2–0.6 s sleep between requests |
| **Connection pooling** | TCP connections reused across parallel threads |
| **Data provenance** | Every record carries `fetch_ts` timestamp and `source_url` |

---

## 8. Stage II — Data Transformation

### Seven-Step Cleaning Protocol

| Step | Operation | Rationale |
|---|---|---|
| 1 | **Duplicate removal** | API pagination can return overlapping records across pages |
| 2 | **Year range filter** | Restrict to configured `start_year`–`end_year` window |
| 3 | **Pivot long → wide** | Convert from (country, year, indicator, value) to (country, year, ind₁...ind₆) |
| 4 | **Linear interpolation** | Vectorised per-country gap filling preserves temporal trend signal |
| 5 | **Forward / back fill** | Handle leading and trailing NaN values after interpolation |
| 6 | **Per-country winsorisation** | Cap values at 1st/99th percentile *within each country* to preserve cross-country structural differences |
| 7 | **Residual NaN drop** | Remove any remaining incomplete rows after all imputation steps |

> **Design note — Per-Country Winsorisation:** Applying outlier capping at the global level would erroneously classify India's population (1.4B) as an outlier relative to Germany's (83M). Winsorising within each country's time series preserves inter-country heterogeneity while removing intra-country measurement artefacts.

---

## 9. Stage III — Data Loading

The cleaned dataset is persisted in two formats simultaneously for maximum downstream compatibility:

| Format | File | Use Case |
|---|---|---|
| **CSV** | `clean_worldbank.csv` | Quick exploration, sharing, Excel analysis |
| **SQLite** | `worldbank.db` | SQL queries, relational joins, programmatic access |

The SQLite database contains two tables: `raw_data` (original API responses) and `clean_data` (post-transformation records), enabling before/after comparison and full audit traceability.

---

## 10. Stage IV — Feature Engineering

Raw indicators span several orders of magnitude (Population ~10⁹ vs Unemployment ~5%), requiring normalisation and domain-informed derived features before mining.

### Derived Features

| Feature | Formula | Rationale |
|---|---|---|
| `GDP_LOG` | log₁₀(GDP_PC + 1) | Compresses the right-skewed GDP distribution |
| `ECON_STABILITY` | GDP_PC / (UNEMP + 1) | Combines growth and employment into a composite score |
| `HUMAN_DEV` | (LIFE_EXP × INTERNET) / 1000 | Proxy for human development index |
| `GROWTH_RATE` | YoY % change in GDP_PC, clipped to [−50, +100] | Momentum signal; clipped to exclude hyperinflation artefacts |
| `HIGH_GDP` | GDP_PC > median → 1, else 0 | Binary classification target |
| `COUNTRY_ENC` | Label-encoded country code | Required for tree-based models |

### Scaling

All features are standardised using `StandardScaler` (zero mean, unit variance) before clustering and classification to prevent high-magnitude features (e.g. Population) from dominating distance calculations.

---

## 11. Stage V — Data Mining

### 11.1 Frequent Pattern Mining — Apriori & FP-Growth

Continuous indicators are discretised into binary `_HIGH` flags (above median) before mining. Only `_HIGH` flags — not complementary `_LOW` pairs — are used, preventing trivially anti-correlated rules that merely restate the definition of binary complements (e.g. `GDP_HIGH → ~GDP_LOW`).

**Metrics computed for every association rule:**

| Metric | Formula | Interpretation |
|---|---|---|
| Support | P(A ∪ B) | Frequency of the itemset in the dataset |
| Confidence | P(B \| A) | Reliability of the rule A → B |
| Lift | conf / P(B) | Surprise factor relative to baseline frequency |
| Conviction | (1 − P(B)) / (1 − conf) | Strength of directional implication |

Both **Apriori** and **FP-Growth** are run with identical thresholds (`min_support = 0.30`, `min_confidence = 0.60`) to allow algorithmic comparison.

### 11.2 Clustering — K-Means with PCA & UMAP

**Methodology:**

1. Select standardised numeric features: `GDP_PC, UNEMP, INFLATION, LIFE_EXP, INTERNET, ECON_STABILITY, HUMAN_DEV`
2. Determine optimal *k* via **Elbow (inertia)** and **Silhouette coefficient** jointly, using a consistent `n_init = 20`
3. Fit final K-Means with `n_clusters = 4`, `random_state = 42`
4. Project to 2D via **PCA** (linear, interpretable variance ratios) and **UMAP** (non-linear, topology-preserving)
5. Profile each cluster via mean indicator values and **Radar charts** (normalised to [0, 1])

### 11.3 Classification — Multi-Model Benchmark

Target: `HIGH_GDP` (1 = GDP per capita above cross-country median)

| Model | Key Hyperparameters |
|---|---|
| Random Forest | 300 trees, max_depth = 10, n_jobs = −1 |
| Gradient Boosting | 200 estimators, learning_rate = 0.05, max_depth = 4 |
| Logistic Regression | C = 1.0, solver = lbfgs, max_iter = 1000 |
| Support Vector Machine | RBF kernel, C = 1.0, probability = True |

All models are evaluated under **Stratified 5-Fold Cross-Validation**. Final test-set evaluation reports:
- Classification report (precision, recall, F1)
- Confusion matrix (normalised)
- ROC curve with AUC score
- Feature importance (Gini) for tree-based models

The best model by cross-validation mean accuracy is serialised to `best_model.joblib` via `joblib`.

---

## 12. Results & Outputs

### Generated Figures

---

#### Figure 1 · Raw Extraction Profile
![fig1_raw_profile](https://github.com/user-attachments/assets/b9e15d2e-bf74-4846-a75c-17580006974c)
> Missing rates per indicator, record counts per country, and records per year — snapshot of the raw API extraction before any cleaning.

---

#### Figure 2 · Before vs. After Cleaning — Distribution Comparison
![fig2_before_after_cleaning](https://github.com/user-attachments/assets/e3ad5a84-7e02-464c-b103-30ef2a0c1584)
> Side-by-side histograms of each indicator before and after the 7-step cleaning protocol, illustrating the effect of winsorisation and interpolation.

---

#### Figure 3 · Feature Correlation Matrices
![fig3_correlation](https://github.com/user-attachments/assets/c0054014-2f51-4398-ba7c-59a4214a48ca)
> Pearson correlation heatmaps for original indicators (left) and the full engineered feature set (right), revealing inter-feature linear dependencies.

---

#### Figure 4 · Time-Series Trends by Country (2000–2023)
![fig4_time_series](https://github.com/user-attachments/assets/d7042706-3470-43da-9951-118cf7e10a63)
> Longitudinal trajectories of all 6 indicators across 7 countries. Vertical annotations mark the 2008 Global Financial Crisis and the 2020 COVID-19 shock.

---

#### Figure 5 · Elbow & Silhouette — Optimal k Selection
![fig5_elbow_silhouette](https://github.com/user-attachments/assets/bf733a27-216b-4f09-8757-892fa1b4c239)
> Joint Elbow (inertia) and Silhouette coefficient curves across k = 2–8, used to determine the optimal number of clusters for K-Means.

---

#### Figure 6 · K-Means Clustering — PCA 2D Projection
![fig6_pca_clusters](https://github.com/user-attachments/assets/5b37a301-d102-43c6-9ad5-8a149a2ed34f)
> Principal Component Analysis projection of all country-year observations, colour-coded by cluster assignment with cluster centroids marked.

---

#### Figure 7 · UMAP Non-Linear Projection
![fig7_umap_clusters](https://github.com/user-attachments/assets/cf589008-e1fe-49d0-ac2a-22ddb35f4f90)
> Uniform Manifold Approximation and Projection (UMAP) embedding preserving local topology. Most-recent year observations are annotated with country ISO codes.

---

#### Figure 8 · Association Rules — Apriori & FP-Growth
![fig8_association_rules](https://github.com/user-attachments/assets/34cfdf20-9d98-411f-a5c6-2a88ab56925a)
> (A) Top rules ranked by Lift. (B) Support vs. Confidence scatter plot sized by Lift. (C) Conviction heatmap across antecedent–consequent pairs.

---

#### Figure 9 · Classification — Confusion Matrices & ROC Curves
![fig9_cm_roc](https://github.com/user-attachments/assets/5afe8d8d-f5ad-42f1-8de6-bb5b64b0eccb)
> Normalised confusion matrices (top row) and ROC curves with AUC scores (bottom row) for all four classifiers: Random Forest, Gradient Boosting, Logistic Regression, and SVM.

---

#### Figure 10 · Feature Importances — Best Classifier
![fig10_feature_importance](https://github.com/user-attachments/assets/82f4925d-6207-4449-adb3-1135fa6584df)
> Gini impurity-based feature importances for the best-performing model, identifying the most predictive macroeconomic signals for the HIGH_GDP classification target.

---

#### Figure 11 · Summary Dashboard
![fig11_dashboard](https://github.com/user-attachments/assets/f89feee5-2571-4241-b75d-431ca28b11bc)
> Consolidated dark-theme dashboard presenting KPI tiles, time-series overview, cluster PCA projection, top association rules, and classification results in a single unified view.

### Key Performance Indicators

| KPI | Value |
|---|---|
| Countries covered | 7 |
| Year span | 2000–2023 |
| Total API endpoints called | 42 |
| Indicators per country | 6 |
| Engineered features | 9 |
| Classification CV folds | 5 (Stratified) |
| Clustering method | K-Means (k = 4) |
| Association rule algorithms | Apriori + FP-Growth |

---

## 13. Reproducibility

The pipeline is fully deterministic given a fixed `random_state`. To reproduce all results from scratch:

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Open and run all cells top to bottom
jupyter notebook WEB_Scraping_ETL_Professional_Enhanced.ipynb
```

All outputs are regenerated in `pipeline_data/`. A ZIP archive of all outputs is produced automatically in the final cell.

> **Reproducibility guarantee:** `random_state = 42` is propagated to all stochastic operations — K-Means initialisation, train/test split, cross-validation shuffling, and all scikit-learn estimators.

---

## 14. Limitations & Future Work

### Current Limitations

- **Country coverage:** The pipeline covers 7 representative countries. Extending to all ~200 World Bank member states would require pagination-aware rate management and longer execution time.
- **Temporal resolution:** Annual data limits detection of sub-annual economic shocks. Quarterly series exist for some indicators but are not universally available.
- **Causal inference:** Association rules and clustering identify co-occurrence and structural similarity but do not establish causality.
- **UMAP availability:** The UMAP projection requires an optional dependency; environments without `umap-learn` skip this visualisation.

### Future Work

- Extend to the full World Bank country roster with distributed fetching
- Incorporate quarterly and monthly indicators for higher-resolution analysis
- Add causal discovery algorithms (e.g. PC algorithm, LiNGAM) alongside associative mining
- Deploy as a scheduled pipeline with automatic refresh and drift detection
- Integrate a Streamlit or Dash dashboard for interactive exploration

---

## 15. References

- World Bank Open Data. (2024). *World Bank Indicators API v2 Documentation*. https://datahelpdesk.worldbank.org/knowledgebase/articles/889392
- Agrawal, R., & Srikant, R. (1994). Fast algorithms for mining association rules. *Proc. VLDB*, 1215, 487–499.
- Han, J., Pei, J., & Yin, Y. (2000). Mining frequent patterns without candidate generation. *ACM SIGMOD Record*, 29(2), 1–12.
- MacQueen, J. (1967). Some methods for classification and analysis of multivariate observations. *Proc. 5th Berkeley Symposium on Mathematical Statistics*, 1, 281–297.
- McInnes, L., Healy, J., & Melville, J. (2018). UMAP: Uniform Manifold Approximation and Projection for Dimension Reduction. *arXiv:1802.03426*.
- Breiman, L. (2001). Random forests. *Machine Learning*, 45(1), 5–32.
- Friedman, J. H. (2001). Greedy function approximation: A gradient boosting machine. *Annals of Statistics*, 29(5), 1189–1232.

---

*Pipeline version: 1.0 — Data sourced from World Bank Open Data API · Last updated: 2024*
