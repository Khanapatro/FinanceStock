<div align="center">

<img src="https://img.shields.io/badge/Project-FinanceStock-0078d4?style=for-the-badge&logoColor=white" />

# 📈 FinanceStock — Real-Time Stock Market Data Platform

**End-to-end Data Engineering pipeline on Azure** — Streaming ingestion, CDC, Medallion Architecture (Bronze → Silver → Gold), and a Star Schema serving layer for production-level portfolio analytics.

<br/>

![Microsoft Azure](https://img.shields.io/badge/Microsoft_Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-E84D0E?style=for-the-badge&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADA6?style=for-the-badge&logo=delta&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

<br/>

![Medallion Layers](https://img.shields.io/badge/Medallion_Layers-3-cd7f32?style=flat-square)
![Fact Tables](https://img.shields.io/badge/Fact_Tables-3-ffd700?style=flat-square)
![Dimension Tables](https://img.shields.io/badge/Dimension_Tables-5-silver?style=flat-square)
![Analytics Queries](https://img.shields.io/badge/Analytics_Queries-6-38bdf8?style=flat-square)
![Dataset Rows](https://img.shields.io/badge/Dataset_Rows-6%2C962-green?style=flat-square)
![License](https://img.shields.io/badge/License-MIT-brightgreen?style=flat-square)

</div>

---

## 🗺️ Architecture Overview

![Architecture Diagram](https://github.com/user-attachments/assets/e60fee24-24cb-4afc-9782-8246e3e1c611)

---

## 🏗️ Tech Stack

| Layer | Tool |
|---|---|
| ☁️ Cloud Platform | Microsoft Azure (ADLS Gen2, Event Hubs) |
| ⚙️ Processing Engine | Apache Spark (PySpark) via Databricks |
| 🗃️ Storage Format | Delta Lake (Bronze / Silver / Gold) |
| 🔴 Streaming Ingestion | Azure Event Hubs (Kafka-compatible) |
| 📥 Batch Ingestion | Auto Loader (`cloudFiles`) |
| 🔄 Orchestration | Apache Airflow |
| 📊 Serving Layer | Star Schema (Fact + Dimension tables) |
| 🐍 Language | Python 3.x |

---

## 📁 Repository Structure

```
FinanceStock/
├── Bronze/
│   └── bronze_ingestion.py        # CSV + Event Hubs → Delta Bronze
├── Silver/                        # Cleaning, casting, dedup notebooks
├── Gold/                          # Aggregations, Star Schema build
├── Datasets/
│   ├── stocks.csv
│   ├── prices.csv
│   ├── portfolio.csv
│   ├── transactions.csv
│   └── market_events.csv
├── DashboardCSVFiles/             # Exported Gold data for BI tools
├── Screenshots/
├── pythonfile/
│   └── stock_event_generator.py  # Publishes events to Azure Event Hubs
└── README.md
```

---

## 🗂️ Dataset Overview

| File | Rows | Description |
|---|---|---|
| `stocks.csv` | 13 | Master stock dimension — ticker, sector, exchange |
| `prices.csv` | 6,240 | Daily OHLCV per stock (Open, High, Low, Close, Volume) |
| `portfolio.csv` | 72 | User holdings — quantity, avg cost, status |
| `transactions.csv` | 533 | Buy/Sell ledger with fees and execution status |
| `market_events.csv` | 104 | Earnings, splits, analyst upgrades/downgrades |

---

## 🏛️ Medallion Architecture

```
┌──────────────────────────────────────────────────────────┐
│  SOURCES                                                 │
│  CSV files (ADLS raw/)    Azure Event Hubs               │
└────────────┬──────────────────────┬──────────────────────┘
             │                      │
             ▼                      ▼
┌──────────────────────────────────────────────────────────┐
│  🟤 BRONZE  — Raw, append-only, no transforms            │
│  Auto Loader cloudFiles → Delta Lake                     │
│  CSV tables  +  Streaming TICK / CDC / OHLCV             │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│  ⚪ SILVER  — Cleaned, typed, deduped, joined            │
│  Cast dates · Dedup on natural keys · MERGE INTO CDC     │
│  Incremental load via _ingested_at watermark             │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│  🟡 GOLD  — Business-ready Star Schema                   │
│  Fact_Transactions · Fact_Prices · Fact_Portfolio_Snap   │
│  Dim_Stock · Dim_User · Dim_Portfolio · Dim_Date · Dim_Event │
└──────────────────────────────────────────────────────────┘
```

---

## ⚡ Event Generator — Azure Event Hubs

The script connects to **Azure Event Hubs** and publishes three real-world financial event types:

| Event | Description |
|---|---|
| `TICK` | Live streaming trade data — price, bid, ask, volume |
| `CDC` | Portfolio DB change events — INSERT / UPDATE / DELETE |
| `OHLCV` | End-of-day candle events — open, high, low, close, volume |

```bash
pip install azure-eventhub
python pythonfile/stock_event_generator.py
# Select mode: 1=Tick  2=CDC  3=OHLCV  4=Mixed
```
---

## 🟡 Gold Layer — Star Schema

```
                    Dim_Stock
                        │
Dim_Date ── Fact_Transactions ── Dim_Portfolio ── Dim_User
                        │
                    Dim_Event

Dim_Date ── Fact_Prices ── Dim_Stock

Dim_Date ── Fact_Portfolio_Snapshot ── Dim_Stock ── Dim_User
```

### Fact Tables

| Table | Grain | Key Measures |
|---|---|---|
| `Fact_Transactions` | One row per transaction | quantity, price, total_value, fee |
| `Fact_Prices` | One stock per day | open, high, low, close, volume |
| `Fact_Portfolio_Snapshot` | Daily holdings per user per stock | quantity, avg_cost, total_invested |

### Dimension Tables

| Table | Key Columns |
|---|---|
| `Dim_Stock` | ticker, company_name, sector, exchange |
| `Dim_User` | user_id |
| `Dim_Portfolio` | portfolio_id, user_id, status |
| `Dim_Date` | full_date, year, month, day, quarter, weekday |
| `Dim_Event` | event_type, impact, price_change_pct, description |

---

## 📊 Business Analytics Queries

### 1. 💰 Profitability Analysis — Top Stocks by Profit

<p align="center">
  <img src="https://github.com/user-attachments/assets/acf6b243-2a81-4636-9bbb-a7e474cce5e4" width="45%"/>
  <img src="https://github.com/user-attachments/assets/2ad0d46f-c053-4afd-9aab-1b075518720d" width="45%"/>
</p>

**Insights:**
- Identifies the most profitable stocks based on net buy vs sell transactions.
- Helps investors understand which assets are generating the highest returns.
- BUY vs SELL distribution across sectors reveals trading behavior trends.
- Sectors with higher SELL activity may indicate profit booking or market exit signals.

---

### 2. 🚀 Market Event Impact & Sector Returns

<p align="center">
  <img src="https://github.com/user-attachments/assets/8fbe6704-6771-4689-a1c6-fb7459f2b9f4" width="45%"/>
  <img src="https://github.com/user-attachments/assets/3ac12f4e-27b0-44b6-95eb-1075dc532673" width="45%"/>
</p>

**Insights:**
- Shows how market events (earnings, splits, upgrades) impact stock prices and trading activity.
- Helps in understanding volatility triggered by external factors.
- Sector monthly returns highlight which industries are outperforming.
- Useful for portfolio rebalancing and sector-based investment strategies.

---

### 3. 👤 Investor Behavior Analysis

<p align="center">
  <img src="https://github.com/user-attachments/assets/5867984a-a262-4762-9ac9-493600567323" width="45%"/>
</p>

**Insights:**
- Classifies users into High-Frequency, Active, and Long-Term investors.
- Helps identify trading patterns and user segmentation.
- Useful for financial platforms to personalize recommendations.
- High-frequency users contribute more to transaction volume.

---

### 4. 📊 Latest Market Snapshot

<p align="center">
  <img src="https://github.com/user-attachments/assets/3aa0599a-2706-4fcf-9dcb-9209ceb7075e" width="45%"/>
</p>

**Insights:**
- Displays the latest stock prices across different symbols.
- Provides a quick overview of current market conditions.
- Helps traders make real-time decisions.
- Useful for dashboards and monitoring tools.

---

### 5. 📈 Daily Return Analysis

<p align="center">
  <img src="https://github.com/user-attachments/assets/654f87f7-c9dd-4c43-8961-55cc4100e888" width="45%"/>
</p>

**Insights:**
- Shows daily percentage returns for each stock.
- Helps identify high volatility stocks.
- Useful for short-term trading strategies.
- Supports risk analysis and performance tracking.

---

## 🔄 Incremental Load Strategy

| Layer | Strategy | Key Column |
|---|---|---|
| Bronze CSV | Auto Loader tracks files via checkpoint | File path |
| Bronze Stream | Event Hubs offset tracking | `_eh_offset` |
| Silver | Watermark on new Bronze rows only | `_ingested_at` |
| Silver CDC | `MERGE INTO` Delta — upsert | `txn_id`, `portfolio_id` |
| Gold | Incremental rebuild by partition | `date_id` |

---

## 🗺️ Azure Storage Layout

```
financestoragebablu/
├── raw/                          ← source CSV drops
│     ├── stocks/
│     ├── prices/
│     ├── portfolio/
│     ├── transactions/
│     └── market_events/
└── bronze/
      ├── tables/                 ← Delta Bronze tables
      ├── streaming/              ← Event Hubs Delta tables
      │     ├── TICK/
      │     ├── CDC/
      │     └── OHLCV/
      └── _checkpoints/          ← checkpoint + schemaLocation
```


---

## 🚀 How to Run

**Prerequisites:** Databricks workspace (Runtime 11.3+), Azure Storage Account, Azure Event Hubs namespace, Python 3.8+

```bash
# Step 1 — Upload datasets to ADLS raw container
az storage blob upload-batch \
  --account-name financestoragebablu \
  --destination "raw/stocks" --source Datasets/ --pattern "stocks*.csv"

# Step 2 — Generate events to Event Hubs
pip install azure-eventhub
python pythonfile/stock_event_generator.py   # Choose mode 4 (Mixed)

# Step 3 — Run Bronze ingestion (Databricks)
# Import Bronze/bronze_ingestion.py → Run All Cells

# Step 4 — Run Silver transforms (Databricks)
# Import Silver/ notebooks → Run All Cells

# Step 5 — Build Gold Star Schema (Databricks)
# Import Gold/ notebooks → Run All Cells
```

---

## 📌 Key Design Decisions

**Why Delta Lake over Parquet?**
ACID transactions, time travel (`VERSION AS OF`), schema evolution, and `MERGE INTO` for CDC upserts.

**Why `availableNow=True` instead of `once=True`?**
`trigger(once=True)` is deprecated in Databricks Runtime 11.3+. `availableNow=True` processes the full backlog then stops gracefully.

**Why schemaLocation inside checkpoint?**
Keeps schema inference and stream offsets co-located. Resetting a checkpoint also resets schema — no orphaned files.

**Why explicit schemas in Bronze?**
Prevents Auto Loader from misreading column types on malformed files. Dates stay as `StringType` in Bronze — Silver does the safe cast.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

<div align="center">
  <sub>Built with Apache Spark · Delta Lake · Azure Event Hubs · Databricks</sub>
</div>
