# 🏦 Banking Modern Data Stack

An end-to-end data engineering project that builds a real-time banking data pipeline using the modern data stack. From secure OLTP transactions to real-time change data capture and analytics-ready data models.

## 🏗️ Architecture

```
┌─────────────┐    CDC     ┌──────────────┐   Stream   ┌──────────┐
│  PostgreSQL  │──────────▶│ Kafka +      │──────────▶│  MinIO   │
│  (OLTP)      │  Debezium │ Debezium     │           │  (S3)    │
└──────┬───────┘           └──────────────┘           └────┬─────┘
       │                                                    │
       │  Faker                              Airflow DAGs   │
       │  Generator                          (Orchestrate)  │
       ▼                                          │         │
┌─────────────┐                                   ▼         │
│  Simulated   │                          ┌──────────────┐  │
│  Banking     │                          │  Snowflake    │◀─┘
│  Data        │                          │  (Warehouse)  │
└─────────────┘                           │  Bronze/Silver│
                                          │  /Gold        │
                                          └──────┬───────┘
                                                 │ dbt
                                                 ▼
                                          ┌──────────────┐
                                          │  Analytics    │
                                          │  (Power BI)   │
                                          └──────────────┘
```

## ⚡ Tech Stack

| Tool | Purpose |
|------|---------|
| **PostgreSQL** | Source OLTP database with ACID guarantees |
| **Apache Kafka + Debezium** | Real-time Change Data Capture (CDC) |
| **MinIO** | S3-compatible data lake storage |
| **Apache Airflow** | Workflow orchestration & scheduling |
| **Snowflake** | Cloud data warehouse (Bronze → Silver → Gold) |
| **dbt** | SQL transformations, testing, SCD2 snapshots |
| **GitHub Actions** | CI/CD automation |
| **Power BI** | Enterprise dashboards |
| **Docker** | Containerized local development |

## 📂 Repository Structure

```
banking-modern-datastack/
├── .github/workflows/          # CI/CD pipelines
├── banking_dbt/                # dbt project
│   ├── models/
│   │   ├── staging/            # Source cleaning
│   │   └── marts/              # Fact & dimension tables
│   ├── snapshots/              # SCD Type-2 history
│   └── dbt_project.yml
├── consumer/                   # Kafka → MinIO consumer
├── data-generator/             # Faker-based data simulator
│   ├── config.yaml             # Generation parameters
│   └── faker_generator.py
├── docker/dags/                # Airflow DAGs
├── kafka-debezium/             # CDC connector configs
├── postgres/schema.sql         # OLTP DDL
├── docker-compose.yml          # Infrastructure
└── requirements.txt
```

## 🚀 Quick Start

### Phase 1: PostgreSQL + Data Generator

```bash
# 1. Start PostgreSQL (WAL configured for CDC)
docker compose up postgres -d

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Seed the database with synthetic banking data
cd data-generator
python faker_generator.py --mode seed

# 4. (Optional) Start streaming new transactions for CDC
python faker_generator.py --mode stream
```

### Verify the seed:

```bash
docker exec -it banking_postgres psql -U banking_user -d banking_oltp -c "
  SELECT 'customers' AS table_name, COUNT(*) FROM customers
  UNION ALL
  SELECT 'accounts', COUNT(*) FROM accounts
  UNION ALL
  SELECT 'transactions', COUNT(*) FROM transactions
  UNION ALL
  SELECT 'loans', COUNT(*) FROM loans;
"
```

### Phase 2: Kafka + Debezium CDC *(coming next)*

### Phase 3: Airflow Orchestration *(coming next)*

### Phase 4: dbt Transformations *(coming next)*

### Phase 5: CI/CD + Power BI *(coming next)*

## 📊 Data Model

### Customers
| Column | Type | Description |
|--------|------|-------------|
| customer_id | UUID | Primary key |
| first_name | VARCHAR | Customer first name |
| last_name | VARCHAR | Customer last name |
| email | VARCHAR | Unique email |
| credit_score | INTEGER | 300-850 range |
| is_active | BOOLEAN | Active status |

### Accounts
| Column | Type | Description |
|--------|------|-------------|
| account_id | UUID | Primary key |
| customer_id | UUID | FK → customers |
| account_type | ENUM | checking, savings, credit, loan, money_market |
| balance | NUMERIC | Current balance |
| status | ENUM | active, inactive, frozen, closed |

### Transactions
| Column | Type | Description |
|--------|------|-------------|
| transaction_id | UUID | Primary key |
| account_id | UUID | FK → accounts |
| transaction_type | ENUM | deposit, withdrawal, transfer, payment, fee, interest, refund |
| amount | NUMERIC | Transaction amount |
| channel | ENUM | branch, atm, online, mobile, wire, ach |
| status | ENUM | pending, completed, failed, reversed |

### Loans
| Column | Type | Description |
|--------|------|-------------|
| loan_id | UUID | Primary key |
| account_id | UUID | FK → accounts |
| principal | NUMERIC | Loan amount |
| interest_rate | NUMERIC | Annual rate |
| term_months | INTEGER | Loan duration |
| status | ENUM | applied → approved → disbursed → repaying → closed |

## 🛠 Development

```bash
# Full infrastructure
docker compose up -d

# Run data generator
cd data-generator && python faker_generator.py --mode both

# dbt (after Phase 4 setup)
cd banking_dbt && dbt run && dbt test
```

## 📌 Key Design Decisions

- **UUIDs** as primary keys — industry standard for distributed systems
- **ENUMs** for status fields — enforces data quality at the source
- **WAL-level logical** — enables Debezium CDC without triggers or polling
- **Weighted distributions** — realistic transaction patterns (not uniform random)
- **Streaming mode** — continuous INSERT/UPDATE generation for live CDC demos


