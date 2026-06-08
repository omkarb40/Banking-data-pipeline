-- ============================================================
-- Banking OLTP Schema — PostgreSQL 15
-- ============================================================
-- Designed for:
--   • Debezium CDC (WAL-based logical replication)
--   • Realistic banking domain modeling
--   • ACID compliance with proper constraints
-- ============================================================

-- ── Extensions ──────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── ENUM Types ──────────────────────────────────────────────
-- Define all ENUMs before table creation.
-- PostgreSQL ENUMs enforce data quality at the storage layer,
-- which is stronger than CHECK constraints on VARCHAR columns.

CREATE TYPE account_type_enum AS ENUM (
    'checking', 'savings', 'credit', 'loan', 'money_market'
);

CREATE TYPE account_status_enum AS ENUM (
    'active', 'inactive', 'frozen', 'closed'
);

CREATE TYPE txn_type_enum AS ENUM (
    'deposit', 'withdrawal', 'transfer', 'payment',
    'fee', 'interest', 'refund'
);

CREATE TYPE txn_status_enum AS ENUM (
    'pending', 'completed', 'failed', 'reversed'
);

CREATE TYPE txn_channel_enum AS ENUM (
    'branch', 'atm', 'online', 'mobile', 'wire', 'ach'
);

CREATE TYPE loan_status_enum AS ENUM (
    'applied', 'approved', 'disbursed', 'repaying', 'closed', 'defaulted'
);

-- ============================================================
-- 1. BRANCHES — Reference table for bank locations
-- ============================================================
-- Differentiator: most tutorial projects skip organizational
-- structure. Real banking systems always track which branch
-- opened an account or processed a transaction.

CREATE TABLE IF NOT EXISTS branches (
    branch_id       SERIAL PRIMARY KEY,
    branch_name     VARCHAR(150)    NOT NULL,
    branch_code     VARCHAR(10)     NOT NULL UNIQUE,
    address         VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(50),
    zip_code        VARCHAR(10),
    phone           VARCHAR(20),
    is_active       BOOLEAN         DEFAULT TRUE,
    opened_date     DATE,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 2. CUSTOMERS
-- ============================================================
CREATE TABLE IF NOT EXISTS customers (
    customer_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    first_name      VARCHAR(100)    NOT NULL,
    last_name       VARCHAR(100)    NOT NULL,
    email           VARCHAR(255)    NOT NULL UNIQUE,
    phone           VARCHAR(20),
    date_of_birth   DATE            NOT NULL,
    address         VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(50),
    zip_code        VARCHAR(10),
    country         VARCHAR(50)     DEFAULT 'US',
    credit_score    INTEGER         CHECK (credit_score BETWEEN 300 AND 850),
    annual_income   NUMERIC(12, 2),
    employment_status VARCHAR(20)   DEFAULT 'employed'
                    CHECK (employment_status IN (
                        'employed', 'self_employed', 'unemployed',
                        'retired', 'student'
                    )),
    risk_rating     VARCHAR(10)     DEFAULT 'medium'
                    CHECK (risk_rating IN ('low', 'medium', 'high')),
    home_branch_id  INTEGER         REFERENCES branches(branch_id),
    is_active       BOOLEAN         DEFAULT TRUE,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 3. ACCOUNTS
-- ============================================================
CREATE TABLE IF NOT EXISTS accounts (
    account_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id     UUID            NOT NULL REFERENCES customers(customer_id),
    account_type    account_type_enum NOT NULL,
    account_number  VARCHAR(20)     NOT NULL UNIQUE,
    balance         NUMERIC(15, 2)  NOT NULL DEFAULT 0.00,
    currency        VARCHAR(3)      DEFAULT 'USD',
    interest_rate   NUMERIC(5, 4)   DEFAULT 0.0000,
    credit_limit    NUMERIC(15, 2),
    overdraft_limit NUMERIC(10, 2)  DEFAULT 0.00,
    status          account_status_enum DEFAULT 'active',
    opened_branch_id INTEGER        REFERENCES branches(branch_id),
    opened_at       TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    closed_at       TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    -- Credit limit only applies to credit accounts
    CONSTRAINT chk_credit_limit CHECK (
        (account_type = 'credit' AND credit_limit IS NOT NULL AND credit_limit > 0)
        OR (account_type != 'credit')
    ),
    -- Closed accounts must have a closed_at timestamp
    CONSTRAINT chk_closed_date CHECK (
        (status = 'closed' AND closed_at IS NOT NULL)
        OR (status != 'closed')
    )
);

-- ============================================================
-- 4. TRANSACTIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id          UUID            NOT NULL REFERENCES accounts(account_id),
    transaction_type    txn_type_enum   NOT NULL,
    amount              NUMERIC(15, 2)  NOT NULL CHECK (amount > 0),
    balance_before      NUMERIC(15, 2),
    balance_after       NUMERIC(15, 2),
    currency            VARCHAR(3)      DEFAULT 'USD',
    status              txn_status_enum DEFAULT 'completed',
    channel             txn_channel_enum NOT NULL,
    description         VARCHAR(500),
    reference_id        VARCHAR(50),
    counterparty_account UUID,
    merchant_name       VARCHAR(200),
    merchant_category   VARCHAR(100),
    is_flagged          BOOLEAN         DEFAULT FALSE,
    flag_reason         VARCHAR(255),
    processed_branch_id INTEGER         REFERENCES branches(branch_id),
    transaction_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 5. LOANS
-- ============================================================
CREATE TABLE IF NOT EXISTS loans (
    loan_id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id      UUID            NOT NULL REFERENCES accounts(account_id),
    customer_id     UUID            NOT NULL REFERENCES customers(customer_id),
    loan_type       VARCHAR(30)     DEFAULT 'personal'
                    CHECK (loan_type IN (
                        'personal', 'auto', 'mortgage',
                        'student', 'business', 'home_equity'
                    )),
    principal       NUMERIC(15, 2)  NOT NULL CHECK (principal > 0),
    interest_rate   NUMERIC(5, 4)   NOT NULL CHECK (interest_rate >= 0),
    term_months     INTEGER         NOT NULL CHECK (term_months > 0),
    monthly_payment NUMERIC(15, 2),
    total_paid      NUMERIC(15, 2)  DEFAULT 0.00,
    outstanding     NUMERIC(15, 2),
    status          loan_status_enum DEFAULT 'applied',
    next_payment_date DATE,
    disbursed_at    TIMESTAMP,
    maturity_date   DATE,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 6. AUDIT LOG — Compliance & Security Trail
-- ============================================================
-- Differentiator: banking systems are regulated. An audit log
-- shows you understand compliance requirements (SOX, PCI-DSS).
-- Debezium captures this table too, giving you CDC events
-- on the audit trail itself — useful for monitoring dashboards.

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id        BIGSERIAL PRIMARY KEY,
    table_name      VARCHAR(50)     NOT NULL,
    record_id       VARCHAR(50)     NOT NULL,
    action          VARCHAR(10)     NOT NULL
                    CHECK (action IN ('INSERT', 'UPDATE', 'DELETE')),
    old_values      JSONB,
    new_values      JSONB,
    changed_by      VARCHAR(100)    DEFAULT CURRENT_USER,
    changed_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    ip_address      VARCHAR(45),
    session_id      VARCHAR(100)
);

-- ============================================================
-- INDEXES
-- ============================================================
-- Foreign key columns (PostgreSQL does NOT auto-index FK cols)
CREATE INDEX idx_customers_home_branch      ON customers(home_branch_id);
CREATE INDEX idx_accounts_customer          ON accounts(customer_id);
CREATE INDEX idx_accounts_opened_branch     ON accounts(opened_branch_id);
CREATE INDEX idx_transactions_account       ON transactions(account_id);
CREATE INDEX idx_transactions_branch        ON transactions(processed_branch_id);
CREATE INDEX idx_loans_customer             ON loans(customer_id);
CREATE INDEX idx_loans_account              ON loans(account_id);

-- Query performance indexes
CREATE INDEX idx_customers_email            ON customers(email);
CREATE INDEX idx_customers_credit_score     ON customers(credit_score);
CREATE INDEX idx_customers_risk_rating      ON customers(risk_rating);
CREATE INDEX idx_accounts_type              ON accounts(account_type);
CREATE INDEX idx_accounts_status            ON accounts(status);
CREATE INDEX idx_transactions_type          ON transactions(transaction_type);
CREATE INDEX idx_transactions_status        ON transactions(status);
CREATE INDEX idx_transactions_channel       ON transactions(channel);
CREATE INDEX idx_transactions_at            ON transactions(transaction_at);
CREATE INDEX idx_transactions_flagged       ON transactions(is_flagged) WHERE is_flagged = TRUE;
CREATE INDEX idx_loans_status               ON loans(status);
CREATE INDEX idx_loans_next_payment         ON loans(next_payment_date);
CREATE INDEX idx_audit_table_record         ON audit_log(table_name, record_id);
CREATE INDEX idx_audit_changed_at           ON audit_log(changed_at);

-- ============================================================
-- TRIGGER — Auto-update updated_at on row modification
-- ============================================================
-- Debezium uses the updated_at column for timestamp-based
-- snapshot strategies in dbt (SCD Type-2).

CREATE OR REPLACE FUNCTION fn_update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW
    EXECUTE FUNCTION fn_update_timestamp();

CREATE TRIGGER trg_accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION fn_update_timestamp();

CREATE TRIGGER trg_loans_updated_at
    BEFORE UPDATE ON loans
    FOR EACH ROW
    EXECUTE FUNCTION fn_update_timestamp();

-- ============================================================
-- TRIGGER — Auto-populate audit_log on sensitive table changes
-- ============================================================
-- Captures old/new row as JSONB. This feeds both the audit
-- trail AND gives Debezium rich change events to stream.

CREATE OR REPLACE FUNCTION fn_audit_log()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit_log (table_name, record_id, action, new_values)
        VALUES (TG_TABLE_NAME, NEW.customer_id::TEXT, 'INSERT', to_jsonb(NEW));
        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit_log (table_name, record_id, action, old_values, new_values)
        VALUES (TG_TABLE_NAME, NEW.customer_id::TEXT, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;

    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit_log (table_name, record_id, action, old_values)
        VALUES (TG_TABLE_NAME, OLD.customer_id::TEXT, 'DELETE', to_jsonb(OLD));
        RETURN OLD;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_audit
    AFTER INSERT OR UPDATE OR DELETE ON customers
    FOR EACH ROW
    EXECUTE FUNCTION fn_audit_log();

-- ============================================================
-- SEED DATA — Branches (reference data)
-- ============================================================
INSERT INTO branches (branch_name, branch_code, address, city, state, zip_code, phone, opened_date)
VALUES
    ('Downtown Main',      'BR001', '100 Main Street',      'New York',     'NY', '10001', '212-555-0100', '2010-01-15'),
    ('Midtown Financial',  'BR002', '500 5th Avenue',       'New York',     'NY', '10036', '212-555-0200', '2012-06-01'),
    ('Boston Commons',     'BR003', '200 Tremont Street',   'Boston',       'MA', '02108', '617-555-0300', '2015-03-20'),
    ('Chicago Loop',       'BR004', '75 E Washington St',   'Chicago',      'IL', '60602', '312-555-0400', '2013-09-10'),
    ('SF Financial',       'BR005', '300 Montgomery St',    'San Francisco','CA', '94104', '415-555-0500', '2016-11-05'),
    ('Miami Beach',        'BR006', '1200 Ocean Drive',     'Miami',        'FL', '33139', '305-555-0600', '2018-02-14'),
    ('Austin Tech Hub',    'BR007', '600 Congress Ave',     'Austin',       'TX', '78701', '512-555-0700', '2020-07-01'),
    ('Seattle Pioneer',    'BR008', '400 Pike Street',      'Seattle',      'WA', '98101', '206-555-0800', '2019-04-22'),
    ('Denver Highland',    'BR009', '1500 Platte Street',   'Denver',       'CO', '80202', '303-555-0900', '2021-01-10'),
    ('Digital Only',       'BR010', NULL,                    NULL,           NULL, NULL,    '800-555-1000', '2022-06-01')
ON CONFLICT (branch_code) DO NOTHING;

-- ============================================================
-- PostgreSQL WAL Configuration (for Debezium CDC)
-- ============================================================
-- These are set via docker-compose command flags:
--   wal_level = logical
--   max_replication_slots = 4
--   max_wal_senders = 4
--
-- To verify after startup:
--   SHOW wal_level;               -- should return 'logical'
--   SHOW max_replication_slots;   -- should return '4'
-- ============================================================