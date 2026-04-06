"""
Banking Data Generator — Faker + PostgreSQL
=============================================
Modes:
  1. SEED   — Bulk-inserts customers, accounts, transactions, loans
  2. STREAM — Continuously generates new transactions & updates (for CDC)

Usage:
  python faker_generator.py --mode seed
  python faker_generator.py --mode stream
  python faker_generator.py --mode both
"""

import os
import sys
import uuid
import time
import random
import argparse
import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

import yaml
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)
random.seed(42)


# ── Helpers ──────────────────────────────────────────────────
def load_config(path: str = "config.yaml") -> dict:
    """Load generation config from YAML."""
    config_path = os.path.join(os.path.dirname(__file__), path)
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_connection(cfg: dict):
    """Create PostgreSQL connection."""
    db = cfg["database"]
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["name"],
        user=db["user"],
        password=os.getenv("POSTGRES_PASSWORD", db["password"]),
    )


def weighted_choice(distribution: dict) -> str:
    """Pick a value based on weighted probability distribution."""
    items = list(distribution.keys())
    weights = list(distribution.values())
    return random.choices(items, weights=weights, k=1)[0]


def random_amount(min_val: float, max_val: float) -> Decimal:
    """Generate a random monetary amount."""
    val = random.uniform(min_val, max_val)
    return Decimal(str(val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def generate_account_number() -> str:
    """Generate a realistic 12-digit account number."""
    return "".join([str(random.randint(0, 9)) for _ in range(12)])


# ── Customer Generator ───────────────────────────────────────
def generate_customer() -> dict:
    """Generate a single customer record."""
    return {
        "customer_id": str(uuid.uuid4()),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.unique.email(),
        "phone": fake.phone_number()[:20],
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=85),
        "address": fake.street_address()[:255],
        "city": fake.city()[:100],
        "state": fake.state_abbr(),
        "zip_code": fake.zipcode(),
        "country": "US",
        "credit_score": random.randint(300, 850),
        "is_active": random.random() > 0.05,  # 95% active
        "created_at": fake.date_time_between(start_date="-3y", end_date="now"),
    }


# ── Account Generator ────────────────────────────────────────
def generate_account(customer_id: str, cfg: dict) -> dict:
    """Generate a single account for a customer."""
    dist = cfg["distributions"]
    acct_type = weighted_choice(dist["account_types"])
    balance_range = dist["balance_ranges"][acct_type]

    balance = random_amount(balance_range["min"], balance_range["max"])

    # Interest rates by type
    interest_rates = {
        "checking": (0.0001, 0.005),
        "savings": (0.01, 0.045),
        "credit": (0.12, 0.25),
        "loan": (0.03, 0.15),
        "money_market": (0.02, 0.05),
    }
    rate_range = interest_rates[acct_type]
    interest_rate = round(random.uniform(*rate_range), 4)

    credit_limit = None
    if acct_type == "credit":
        credit_limit = random_amount(5000, 50000)

    return {
        "account_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "account_type": acct_type,
        "account_number": generate_account_number(),
        "balance": balance,
        "currency": "USD",
        "interest_rate": interest_rate,
        "credit_limit": credit_limit,
        "status": "active",
        "opened_at": fake.date_time_between(start_date="-2y", end_date="now"),
    }


# ── Transaction Generator ────────────────────────────────────
def generate_transaction(account_id: str, current_balance: Decimal, cfg: dict) -> dict:
    """Generate a single transaction for an account."""
    dist = cfg["distributions"]
    txn_type = weighted_choice(dist["transaction_types"])
    channel = weighted_choice(dist["channels"])

    amount_range = dist["transaction_amounts"][txn_type]
    amount = random_amount(amount_range["min"], amount_range["max"])

    # Calculate balance after transaction
    if txn_type in ("deposit", "interest", "refund"):
        balance_after = current_balance + amount
    elif txn_type in ("withdrawal", "payment", "fee"):
        balance_after = current_balance - amount
    else:  # transfer
        balance_after = current_balance - amount

    # Determine status (small chance of failure)
    status = "completed"
    if random.random() < 0.02:
        status = "failed"
        balance_after = current_balance  # no change on failure
    elif random.random() < 0.01:
        status = "pending"

    # Description templates
    descriptions = {
        "deposit": [
            "Direct deposit - payroll",
            "Cash deposit at branch",
            "Mobile check deposit",
            "ACH transfer received",
        ],
        "withdrawal": [
            "ATM withdrawal",
            "Cash withdrawal at branch",
            "Debit card purchase",
        ],
        "transfer": [
            "Internal transfer",
            "Wire transfer outgoing",
            "Zelle payment sent",
            "ACH transfer sent",
        ],
        "payment": [
            "Bill payment - utilities",
            "Credit card payment",
            "Loan payment",
            "Insurance premium",
            "Mortgage payment",
        ],
        "fee": [
            "Monthly maintenance fee",
            "Overdraft fee",
            "Wire transfer fee",
            "ATM surcharge",
        ],
        "interest": [
            "Monthly interest credit",
            "Savings interest accrual",
        ],
        "refund": [
            "Merchant refund",
            "Fee reversal",
            "Dispute credit",
        ],
    }

    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "transaction_type": txn_type,
        "amount": amount,
        "balance_after": balance_after,
        "currency": "USD",
        "status": status,
        "channel": channel,
        "description": random.choice(descriptions.get(txn_type, ["Transaction"])),
        "reference_id": fake.bothify("REF-####-????").upper() if random.random() > 0.5 else None,
        "counterparty_account": str(uuid.uuid4()) if txn_type == "transfer" else None,
        "transaction_at": fake.date_time_between(start_date="-1y", end_date="now"),
    }


# ── Loan Generator ───────────────────────────────────────────
def generate_loan(account_id: str, customer_id: str) -> dict:
    """Generate a loan record."""
    principal = random_amount(5000, 500000)
    rate = round(random.uniform(0.03, 0.15), 4)
    term = random.choice([12, 24, 36, 48, 60, 120, 180, 240, 360])

    # Simple monthly payment calculation
    monthly_rate = rate / 12
    if monthly_rate > 0:
        payment = float(principal) * (monthly_rate * (1 + monthly_rate) ** term) / (
            (1 + monthly_rate) ** term - 1
        )
    else:
        payment = float(principal) / term
    monthly_payment = Decimal(str(payment)).quantize(Decimal("0.01"))

    # Outstanding is between 0% and 100% of principal
    outstanding = random_amount(0, float(principal))

    statuses = ["applied", "approved", "disbursed", "repaying", "closed", "defaulted"]
    weights = [0.05, 0.05, 0.10, 0.50, 0.25, 0.05]
    status = random.choices(statuses, weights=weights, k=1)[0]

    disbursed_at = None
    if status in ("disbursed", "repaying", "closed", "defaulted"):
        disbursed_at = fake.date_time_between(start_date="-2y", end_date="-3m")

    return {
        "loan_id": str(uuid.uuid4()),
        "account_id": account_id,
        "customer_id": customer_id,
        "principal": principal,
        "interest_rate": rate,
        "term_months": term,
        "monthly_payment": monthly_payment,
        "outstanding": outstanding,
        "status": status,
        "disbursed_at": disbursed_at,
    }


# ── Seed Mode ─────────────────────────────────────────────────
def seed_data(conn, cfg: dict):
    """Bulk-insert initial dataset."""
    gen = cfg["generation"]
    dist = cfg["distributions"]
    cur = conn.cursor()

    # ── Customers ──
    num_customers = gen["customers"]
    logger.info(f"Generating {num_customers} customers...")
    customers = [generate_customer() for _ in range(num_customers)]

    execute_values(
        cur,
        """
        INSERT INTO customers (customer_id, first_name, last_name, email, phone,
            date_of_birth, address, city, state, zip_code, country,
            credit_score, is_active, created_at)
        VALUES %s
        ON CONFLICT (email) DO NOTHING
        """,
        [
            (
                c["customer_id"], c["first_name"], c["last_name"], c["email"],
                c["phone"], c["date_of_birth"], c["address"], c["city"],
                c["state"], c["zip_code"], c["country"], c["credit_score"],
                c["is_active"], c["created_at"],
            )
            for c in customers
        ],
    )
    conn.commit()
    logger.info(f"  ✅ Inserted {num_customers} customers")

    # ── Accounts ──
    all_accounts = []
    for cust in customers:
        num_accts = random.randint(
            gen["accounts_per_customer"]["min"],
            gen["accounts_per_customer"]["max"],
        )
        for _ in range(num_accts):
            acct = generate_account(cust["customer_id"], cfg)
            all_accounts.append(acct)

    logger.info(f"Generating {len(all_accounts)} accounts...")
    execute_values(
        cur,
        """
        INSERT INTO accounts (account_id, customer_id, account_type, account_number,
            balance, currency, interest_rate, credit_limit, status, opened_at)
        VALUES %s
        ON CONFLICT (account_number) DO NOTHING
        """,
        [
            (
                a["account_id"], a["customer_id"], a["account_type"],
                a["account_number"], a["balance"], a["currency"],
                a["interest_rate"], a["credit_limit"], a["status"], a["opened_at"],
            )
            for a in all_accounts
        ],
    )
    conn.commit()
    logger.info(f"  ✅ Inserted {len(all_accounts)} accounts")

    # ── Transactions ──
    all_txns = []
    for acct in all_accounts:
        num_txns = random.randint(
            gen["transactions_per_account"]["min"],
            gen["transactions_per_account"]["max"],
        )
        running_balance = acct["balance"]
        for _ in range(num_txns):
            txn = generate_transaction(acct["account_id"], running_balance, cfg)
            all_txns.append(txn)
            if txn["status"] == "completed":
                running_balance = txn["balance_after"]

    logger.info(f"Generating {len(all_txns)} transactions...")
    execute_values(
        cur,
        """
        INSERT INTO transactions (transaction_id, account_id, transaction_type,
            amount, balance_after, currency, status, channel, description,
            reference_id, counterparty_account, transaction_at)
        VALUES %s
        """,
        [
            (
                t["transaction_id"], t["account_id"], t["transaction_type"],
                t["amount"], t["balance_after"], t["currency"], t["status"],
                t["channel"], t["description"], t["reference_id"],
                t["counterparty_account"], t["transaction_at"],
            )
            for t in all_txns
        ],
    )
    conn.commit()
    logger.info(f"  ✅ Inserted {len(all_txns)} transactions")

    # ── Loans ──
    loan_accounts = [
        a for a in all_accounts if a["account_type"] == "loan"
    ]
    # Also add some loans for non-loan accounts (personal loans)
    extra_loan_candidates = random.sample(
        [a for a in all_accounts if a["account_type"] != "loan"],
        k=min(int(num_customers * gen["loans_probability"]), len(all_accounts)),
    )
    loan_accounts.extend(extra_loan_candidates)

    loans = [
        generate_loan(a["account_id"], a["customer_id"]) for a in loan_accounts
    ]

    if loans:
        logger.info(f"Generating {len(loans)} loans...")
        execute_values(
            cur,
            """
            INSERT INTO loans (loan_id, account_id, customer_id, principal,
                interest_rate, term_months, monthly_payment, outstanding,
                status, disbursed_at)
            VALUES %s
            """,
            [
                (
                    l["loan_id"], l["account_id"], l["customer_id"],
                    l["principal"], l["interest_rate"], l["term_months"],
                    l["monthly_payment"], l["outstanding"], l["status"],
                    l["disbursed_at"],
                )
                for l in loans
            ],
        )
        conn.commit()
        logger.info(f"  ✅ Inserted {len(loans)} loans")

    logger.info("=" * 50)
    logger.info("SEED COMPLETE")
    logger.info(f"  Customers:    {num_customers}")
    logger.info(f"  Accounts:     {len(all_accounts)}")
    logger.info(f"  Transactions: {len(all_txns)}")
    logger.info(f"  Loans:        {len(loans)}")
    logger.info("=" * 50)

    cur.close()


# ── Stream Mode ───────────────────────────────────────────────
def stream_data(conn, cfg: dict):
    """Continuously generate new data to trigger CDC events."""
    gen = cfg["generation"]["streaming"]
    cur = conn.cursor()

    logger.info("Starting STREAM mode (Ctrl+C to stop)...")
    logger.info(f"  Batch size:    {gen['batch_size']}")
    logger.info(f"  Interval:      {gen['interval_seconds']}s")
    logger.info(f"  Update prob:   {gen['update_probability']}")

    batch_num = 0
    try:
        while True:
            batch_num += 1
            logger.info(f"── Batch #{batch_num} ──")

            # Fetch random active accounts
            cur.execute(
                "SELECT account_id, balance FROM accounts WHERE status = 'active' ORDER BY RANDOM() LIMIT %s",
                (gen["batch_size"],),
            )
            accounts = cur.fetchall()

            for account_id, balance in accounts:
                if random.random() < gen["update_probability"]:
                    # UPDATE existing account (triggers CDC update event)
                    new_score = random.randint(300, 850)
                    cur.execute(
                        """
                        UPDATE customers SET credit_score = %s
                        WHERE customer_id = (
                            SELECT customer_id FROM accounts WHERE account_id = %s
                        )
                        """,
                        (new_score, str(account_id)),
                    )
                    logger.info(f"  📝 Updated credit score for account {str(account_id)[:8]}...")
                else:
                    # INSERT new transaction
                    txn = generate_transaction(str(account_id), Decimal(str(balance)), cfg)
                    cur.execute(
                        """
                        INSERT INTO transactions (transaction_id, account_id, transaction_type,
                            amount, balance_after, currency, status, channel, description,
                            reference_id, counterparty_account, transaction_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            txn["transaction_id"], txn["account_id"],
                            txn["transaction_type"], txn["amount"],
                            txn["balance_after"], txn["currency"], txn["status"],
                            txn["channel"], txn["description"], txn["reference_id"],
                            txn["counterparty_account"], txn["transaction_at"],
                        ),
                    )

                    # Update account balance
                    if txn["status"] == "completed":
                        cur.execute(
                            "UPDATE accounts SET balance = %s WHERE account_id = %s",
                            (txn["balance_after"], str(account_id)),
                        )
                    logger.info(
                        f"  💰 {txn['transaction_type']} ${txn['amount']} on {str(account_id)[:8]}..."
                    )

            # Occasionally add a new customer
            if random.random() < gen["new_customer_probability"]:
                cust = generate_customer()
                cur.execute(
                    """
                    INSERT INTO customers (customer_id, first_name, last_name, email, phone,
                        date_of_birth, address, city, state, zip_code, country,
                        credit_score, is_active, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (email) DO NOTHING
                    """,
                    (
                        cust["customer_id"], cust["first_name"], cust["last_name"],
                        cust["email"], cust["phone"], cust["date_of_birth"],
                        cust["address"], cust["city"], cust["state"],
                        cust["zip_code"], cust["country"], cust["credit_score"],
                        cust["is_active"], cust["created_at"],
                    ),
                )
                acct = generate_account(cust["customer_id"], cfg)
                cur.execute(
                    """
                    INSERT INTO accounts (account_id, customer_id, account_type, account_number,
                        balance, currency, interest_rate, credit_limit, status, opened_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (account_number) DO NOTHING
                    """,
                    (
                        acct["account_id"], acct["customer_id"], acct["account_type"],
                        acct["account_number"], acct["balance"], acct["currency"],
                        acct["interest_rate"], acct["credit_limit"], acct["status"],
                        acct["opened_at"],
                    ),
                )
                logger.info(f"  🆕 New customer: {cust['first_name']} {cust['last_name']}")

            conn.commit()
            logger.info(f"  ✅ Batch #{batch_num} committed")
            time.sleep(gen["interval_seconds"])

    except KeyboardInterrupt:
        logger.info("\n⏹ Stream stopped by user.")
    finally:
        cur.close()


# ── Main ──────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Banking Data Generator")
    parser.add_argument(
        "--mode",
        choices=["seed", "stream", "both"],
        default="seed",
        help="seed = bulk insert, stream = continuous CDC, both = seed then stream",
    )
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    args = parser.parse_args()

    cfg = load_config(args.config)
    conn = get_connection(cfg)

    try:
        if args.mode in ("seed", "both"):
            seed_data(conn, cfg)
        if args.mode in ("stream", "both"):
            stream_data(conn, cfg)
    finally:
        conn.close()
        logger.info("Connection closed.")


if __name__ == "__main__":
    main()
