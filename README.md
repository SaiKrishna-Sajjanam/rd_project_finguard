# FinGuard — Real-Time Fraud Detection & Transaction Monitoring

FinGuard is an end-to-end real-time fraud detection and transaction monitoring platform built on Databricks. It ingests banking data (customers, transactions, and a fraud watchlist), processes it through a medallion architecture (bronze → silver → gold) using Lakeflow Spark Declarative Pipelines (SDP), detects fraudulent and high-value transactions in real time, and notifies affected customers by email — with everything visualized on an AI/BI dashboard.

## What It Does

- **Streaming ingestion** — lands raw customers, transactions, and fraud-watchlist data into the `finguard.bronze` schema via Auto Loader.
- **Cleansed silver layer** — deduplicated, typed data in `finguard.silver` (e.g. customer dates cast to DATE, ingestion timestamps added).
- **Real-time fraud detection (gold)** — joins streaming transactions against the fraud watchlist (card numbers) to raise CRITICAL fraud alerts (`finguard.gold.fraud_card_alert`), and flags high-value transactions for review (`finguard.gold.high_value_transactions_alert`).
- **Transaction velocity analytics** — 1-minute tumbling windows and 5-minute sliding windows over transaction counts.
- **Email alerting** — ForEachBatch sinks send fraud and high-value alerts directly to each customer's email. Card numbers are masked to the last 4 digits; sender credentials live in a Databricks secret scope — never in code.
- **Executive dashboard** — a 4-page AI/BI dashboard (*Finguard Fraud & Transaction Monitoring*) covering Executive Overview, Fraud Alerts Deep Dive, Transactions & Customers, and Transaction Velocity.

## Project Structure

```text
finguard_project/
├── 01_kafka_streaming_test.ipynb        # Kafka streaming experiments
├── 02_Setup_Secret_Scope.ipynb          # Secret-scope setup for email credentials
├── Autoloader_test.ipynb                # Auto Loader experiments
├── Finguard Fraud & Transaction Monitoring.lvdash.json   # AI/BI dashboard
├── fraud_watchlist_file_generator/      # Sample watchlist data (CSV)
├── finguard_customers_silver_ingestion_2/
│   └── silver/customers_silver.py       # Customers silver ingestion
└── finguard_streamig/                    # Main streaming pipeline code
    ├── bronze/                          # Raw ingestion (Auto Loader)
    ├── silver/                          # Cleansed & typed tables
    ├── gold/                            # Fraud alerts, high-value alerts, windowed counts
    └── alert/                           # ForEachBatch email alert sinks
```

## Architecture

1. **Bronze** — Auto Loader ingests customers, transactions, and fraud-watchlist files from Unity Catalog volumes into `finguard.bronze`.
2. **Silver** — streaming tables are cleaned, typed, and deduplicated into `finguard.silver`.
3. **Gold** — stream-stream joins with watermarks produce fraud alerts (transactions vs. watchlist), high-value transaction alerts, and per-minute / sliding-window transaction counts in `finguard.gold`.
4. **Alerting** — ForEachBatch append flows read the gold alert tables and email each affected customer via SMTP (credentials from the `finguard-scope` secret scope).
5. **Dashboard** — the AI/BI dashboard in this repo visualizes KPIs, fraud alerts, customer/transaction views, and transaction velocity.

## Prerequisites

- A Databricks workspace with Unity Catalog (catalog: `finguard`)
- A secret scope (`finguard-scope`) with:
  - `gmail_sender_email` — the alert sender address
  - `gmail_api_key` — the sender's app password
- Source data volumes under `finguard.source` (transactions, fraud_watchlist, customers)

## Getting Started

1. Run the `02_Setup_Secret_Scope` notebook to configure the email credentials.
2. Create a Lakeflow Spark Declarative Pipeline pointing at the code in `finguard_streamig/`, `finguard_customers_silver_ingestion_2/`, with target schemas `finguard.bronze`, `finguard.silver`, `finguard.gold`.
3. Start the pipeline — bronze, silver, and gold tables plus the email alert sinks are created automatically.
4. Import `Finguard Fraud & Transaction Monitoring.lvdash.json` as an AI/BI dashboard to explore the results.

## Notes

- No credentials or secrets are stored in this repository.
- `transaction_timestamp` is a STRING in silver and some gold tables; windowed aggregations cast it to TIMESTAMP before applying watermarks.
- All `@dlt.table` / `@dp.table` names are fully qualified (e.g. `finguard.gold.fraud_card_alert`) so tables always land in the intended schema.
