"""
Banking Data Generator — Faker + PostgreSQL
=============================================
Modes:
  seed   — Bulk-insert customers, accounts, transactions, loans
  stream — Continuously generate new events for CDC
  both   — Seed first, then stream

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
from dateutil.relativedelta import relativedelta

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
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_connection(cfg: dict):
    db = cfg["database"]
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["name"],
        user=db["user"],
        password=os.getenv("POSTGRES_PASSWORD", db["password"]),
    )


def weighted_choice(distribution: dict) -> str:
    items = list(distribution.keys())
    weights = list(distribution.values())
    return random.choices(items, weights=weights, k=1)[0]


def random_amount(min_val: float, max_val: float) -> Decimal:
    val = random.uniform(min_val, max_val)
    return Decimal(str(val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def generate_account_number() -> str:
    prefix = random.choice(["4", "5", "6"])  # realistic card-like prefix
    return prefix + "".join([str(random.randint(0, 9)) for _ in range(11)])


# ── Merchant Generator ───────────────────────────────────────
MERCHANT_NAMES = {
    "groceries": [
        "Whole Foods Market", "Trader Joe's", "Kroger",
        "Safeway", "Costco Wholesale", "Aldi", "Publix",
    ],
    "restaurants": [
        "Chipotle Mexican Grill", "Olive Garden", "Panera Bread",
        "Starbucks", "Dunkin' Donuts", "Chick-fil-A", "Subway",
    ],
    "gas_station": [
        "Shell Oil", "Chevron", "BP Amoco",
        "ExxonMobil", "Sunoco", "Speedway",
    ],
    "online_shopping": [
        "Amazon.com", "Walmart.com", "Target.com",
        "eBay Inc", "Etsy Inc", "Best Buy Online",
    ],
    "utilities": [
        "ConEdison Electric", "National Grid Gas",
        "AT&T Wireless", "Verizon Communications",
        "Comcast Cable", "Water Authority",
    ],
    "healthcare": [
        "CVS Pharmacy", "Walgreens", "UnitedHealth",
        "Aetna Insurance", "Quest Diagnostics",
    ],
    "entertainment": [
        "Netflix Inc", "Spotify USA", "AMC Theatres",
        "Ticketmaster", "Sony PlayStation",
    ],
    "travel": [
        "Delta Air Lines", "United Airlines",
        "Marriott Hotels", "Hilton Hotels", "Uber Technologies",
    ],
    "education": [
        "Coursera Inc", "Udemy Inc", "Barnes & Noble",
        "University Bookstore", "Pearson Education",
    ],
    "other": [
        "General Merchandise", "Miscellaneous Retail",
        "Professional Services", "Home Depot", "Lowe's",
    ],
}


def get_merchant(category: str) -> str:
    names = MERCHANT_NAMES.get(category, MERCHANT_NAMES["other"])
    return random.choice(names)


# ── Transaction Description Templates ────────────────────────
TXN_DESCRIPTIONS = {
    "deposit": [
        "Direct deposit - payroll",
        "Cash deposit at branch",
        "Mobile check deposit",
        "ACH transfer received",
        "Wire transfer received",
    ],
    "withdrawal": [
        "ATM withdrawal",
        "Cash withdrawal at branch",
        "Debit card purchase",
        "Counter withdrawal",
    ],
    "transfer": [
        "Internal account transfer",
        "Wire transfer outgoing",
        "Zelle payment sent",
        "ACH transfer sent",
        "Interbank transfer",
    ],
    "payment": [
        "Bill payment - utilities",
        "Credit card payment",
        "Loan payment",
        "Insurance premium",
        "Mortgage payment",
        "Rent payment",
    ],
    "fee": [
        "Monthly maintenance fee",
        "Overdraft fee",
        "Wire transfer fee",
        "ATM surcharge",
        "Foreign transaction fee",
    ],
    "interest": [
        "Monthly interest credit",
        "Savings interest accrual",
        "Money market interest",
    ],
    "refund": [
        "Merchant refund",
        "Fee reversal",
        "Dispute credit",
        "Return credit",
    ],
}


# ── Customer Generator ───────────────────────────────────────
def generate_customer(cfg: dict, branch_ids: list) -> dict:
    dist = cfg["distributions"]
    employment = weighted_choice(dist["employment_statuses"])
    income_range = dist["annual_income_ranges"][employment]

    # Credit score correlates loosely with employment
    base_score = {
        "employed": (580, 850),
        "self_employed": (550, 820),
        "retired": (620, 850),
        "student": (300, 700),
        "unemployed": (300, 650),
    }
    score_range = base_score[employment]

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
        "credit_score": random.randint(*score_range),
        "annual_income": random_amount(income_range["min"], income_range["max"]),
        "employment_status": employment,
        "risk_rating": weighted_choice(dist["risk_ratings"]),
        "home_branch_id": random.choice(branch_ids),
        "is_active": random.random() > 0.05,
        "created_at": fake.date_time_between(start_date="-3y", end_date="now"),
    }


# ── Account Generator ────────────────────────────────────────
def generate_account(customer_id: str, cfg: dict, branch_ids: list) -> dict:
    dist = cfg["distributions"]
    acct_type = weighted_choice(dist["account_types"])
    balance_range = dist["balance_ranges"][acct_type]
    balance = random_amount(balance_range["min"], balance_range["max"])

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

    overdraft_limit = Decimal("0.00")
    if acct_type == "checking":
        overdraft_limit = random.choice(
            [Decimal("0.00"), Decimal("200.00"), Decimal("500.00"), Decimal("1000.00")]
        )

    return {
        "account_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "account_type": acct_type,
        "account_number": generate_account_number(),
        "balance": balance,
        "currency": "USD",
        "interest_rate": interest_rate,
        "credit_limit": credit_limit,
        "overdraft_limit": overdraft_limit,
        "status": "active",
        "opened_branch_id": random.choice(branch_ids),
        "opened_at": fake.date_time_between(start_date="-2y", end_date="now"),
    }


# ── Transaction Generator ────────────────────────────────────
def generate_transaction(
    account_id: str,
    current_balance: Decimal,
    cfg: dict,
    branch_ids: list,
) -> dict:
    dist = cfg["distributions"]
    gen = cfg["generation"]
    txn_type = weighted_choice(dist["transaction_types"])
    channel = weighted_choice(dist["channels"])

    amount_range = dist["transaction_amounts"][txn_type]
    amount = random_amount(amount_range["min"], amount_range["max"])

    balance_before = current_balance

    # Calculate balance after transaction
    if txn_type in ("deposit", "interest", "refund"):
        balance_after = current_balance + amount
    else:
        balance_after = current_balance - amount

    # Determine status
    status = "completed"
    if random.random() < 0.02:
        status = "failed"
        balance_after = current_balance
    elif random.random() < 0.01:
        status = "pending"

    # Merchant info (for payment/withdrawal/refund types)
    merchant_category = None
    merchant_name = None
    if txn_type in ("payment", "withdrawal", "refund"):
        merchant_category = weighted_choice(dist["merchant_categories"])
        merchant_name = get_merchant(merchant_category)

    # Flagging logic
    is_flagged = False
    flag_reason = None
    flag_prob = gen.get("streaming", {}).get("flag_probability", 0.03)
    if random.random() < flag_prob or amount > Decimal("8000"):
        is_flagged = True
        flag_reason = random.choice(dist.get("flag_reasons", ["Automated flag"]))

    # Branch for branch/atm channels
    processed_branch_id = None
    if channel in ("branch", "atm"):
        processed_branch_id = random.choice(branch_ids)

    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "transaction_type": txn_type,
        "amount": amount,
        "balance_before": balance_before,
        "balance_after": balance_after,
        "currency": "USD",
        "status": status,
        "channel": channel,
        "description": random.choice(TXN_DESCRIPTIONS.get(txn_type, ["Transaction"])),
        "reference_id": (
            fake.bothify("REF-####-????").upper() if random.random() > 0.5 else None
        ),
        "counterparty_account": (
            str(uuid.uuid4()) if txn_type == "transfer" else None
        ),
        "merchant_name": merchant_name,
        "merchant_category": merchant_category,
        "is_flagged": is_flagged,
        "flag_reason": flag_reason,
        "processed_branch_id": processed_branch_id,
        "transaction_at": fake.date_time_between(start_date="-1y", end_date="now"),
    }


# ── Loan Generator ───────────────────────────────────────────
def generate_loan(account_id: str, customer_id: str, cfg: dict) -> dict:
    dist = cfg["distributions"]
    loan_type = weighted_choice(dist["loan_types"])

    # Principal ranges by loan type
    principal_ranges = {
        "personal": (2000, 50000),
        "auto": (10000, 80000),
        "mortgage": (100000, 500000),
        "student": (5000, 100000),
        "business": (25000, 250000),
        "home_equity": (20000, 150000),
    }
    p_range = principal_ranges[loan_type]
    principal = random_amount(*p_range)

    # Interest rate ranges by type
    rate_ranges = {
        "personal": (0.06, 0.18),
        "auto": (0.03, 0.10),
        "mortgage": (0.03, 0.08),
        "student": (0.04, 0.08),
        "business": (0.05, 0.15),
        "home_equity": (0.04, 0.09),
    }
    r_range = rate_ranges[loan_type]
    rate = round(random.uniform(*r_range), 4)

    # Term by type
    term_options = {
        "personal": [12, 24, 36, 48, 60],
        "auto": [36, 48, 60, 72],
        "mortgage": [180, 240, 360],
        "student": [60, 120, 180, 240],
        "business": [12, 24, 36, 60, 84],
        "home_equity": [60, 120, 180],
    }
    term = random.choice(term_options[loan_type])

    # Monthly payment calculation (standard amortization)
    monthly_rate = rate / 12
    if monthly_rate > 0:
        payment = float(principal) * (
            monthly_rate * (1 + monthly_rate) ** term
        ) / ((1 + monthly_rate) ** term - 1)
    else:
        payment = float(principal) / term
    monthly_payment = Decimal(str(payment)).quantize(Decimal("0.01"))

    # Status distribution
    statuses = ["applied", "approved", "disbursed", "repaying", "closed", "defaulted"]
    weights = [0.05, 0.05, 0.10, 0.50, 0.25, 0.05]
    status = random.choices(statuses, weights=weights, k=1)[0]

    # Derived dates and amounts based on status
    disbursed_at = None
    total_paid = Decimal("0.00")
    outstanding = principal
    now = datetime.now()

    if status in ("disbursed", "repaying", "closed", "defaulted"):
        disbursed_at = fake.date_time_between(start_date="-2y", end_date="-3m")
        months_elapsed = (now.year - disbursed_at.year) * 12 + (
            now.month - disbursed_at.month
        )
        payments_made = min(months_elapsed, term)

        if status == "closed":
            total_paid = monthly_payment * term
            outstanding = Decimal("0.00")
        elif status == "defaulted":
            payments_made = int(payments_made * random.uniform(0.2, 0.6))
            total_paid = monthly_payment * payments_made
            outstanding = principal - total_paid + random_amount(100, 5000)
        else:
            total_paid = monthly_payment * payments_made
            outstanding = max(principal - total_paid, Decimal("0.00"))

    total_paid = total_paid.quantize(Decimal("0.01"))
    outstanding = outstanding.quantize(Decimal("0.01"))

    # Next payment date (for active loans)
    next_payment_date = None
    if status in ("disbursed", "repaying"):
        next_payment_date = (now + relativedelta(months=1)).replace(day=1).date()

    # Maturity date
    maturity_date = None
    if disbursed_at:
        maturity_date = (disbursed_at + relativedelta(months=term)).date()

    return {
        "loan_id": str(uuid.uuid4()),
        "account_id": account_id,
        "customer_id": customer_id,
        "loan_type": loan_type,
        "principal": principal,
        "interest_rate": rate,
        "term_months": term,
        "monthly_payment": monthly_payment,
        "total_paid": total_paid,
        "outstanding": outstanding,
        "status": status,
        "next_payment_date": next_payment_date,
        "disbursed_at": disbursed_at,
        "maturity_date": maturity_date,
    }


# ── Fetch Branch IDs ─────────────────────────────────────────
def fetch_branch_ids(conn) -> list:
    cur = conn.cursor()
    cur.execute("SELECT branch_id FROM branches")
    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    if not ids:
        raise RuntimeError(
            "No branches found. Ensure schema.sql has run and seeded the branches table."
        )
    logger.info(f"Loaded {len(ids)} branch IDs from database")
    return ids


# ══════════════════════════════════════════════════════════════
# SEED MODE
# ══════════════════════════════════════════════════════════════
def seed_data(conn, cfg: dict):
    gen = cfg["generation"]
    cur = conn.cursor()
    branch_ids = fetch_branch_ids(conn)

    # ── Customers ────────────────────────────────────────────
    num_customers = gen["customers"]
    logger.info(f"Generating {num_customers} customers...")
    customers = [generate_customer(cfg, branch_ids) for _ in range(num_customers)]

    execute_values(
        cur,
        """
        INSERT INTO customers (
            customer_id, first_name, last_name, email, phone,
            date_of_birth, address, city, state, zip_code, country,
            credit_score, annual_income, employment_status, risk_rating,
            home_branch_id, is_active, created_at
        ) VALUES %s
        ON CONFLICT (email) DO NOTHING
        """,
        [
            (
                c["customer_id"], c["first_name"], c["last_name"], c["email"],
                c["phone"], c["date_of_birth"], c["address"], c["city"],
                c["state"], c["zip_code"], c["country"], c["credit_score"],
                c["annual_income"], c["employment_status"], c["risk_rating"],
                c["home_branch_id"], c["is_active"], c["created_at"],
            )
            for c in customers
        ],
    )
    conn.commit()
    logger.info(f"  ✅ Inserted {num_customers} customers")

    # ── Accounts ─────────────────────────────────────────────
    all_accounts = []
    for cust in customers:
        num_accts = random.randint(
            gen["accounts_per_customer"]["min"],
            gen["accounts_per_customer"]["max"],
        )
        for _ in range(num_accts):
            acct = generate_account(cust["customer_id"], cfg, branch_ids)
            all_accounts.append(acct)

    logger.info(f"Generating {len(all_accounts)} accounts...")
    execute_values(
        cur,
        """
        INSERT INTO accounts (
            account_id, customer_id, account_type, account_number,
            balance, currency, interest_rate, credit_limit,
            overdraft_limit, status, opened_branch_id, opened_at
        ) VALUES %s
        ON CONFLICT (account_number) DO NOTHING
        """,
        [
            (
                a["account_id"], a["customer_id"], a["account_type"],
                a["account_number"], a["balance"], a["currency"],
                a["interest_rate"], a["credit_limit"], a["overdraft_limit"],
                a["status"], a["opened_branch_id"], a["opened_at"],
            )
            for a in all_accounts
        ],
    )
    conn.commit()
    logger.info(f"  ✅ Inserted {len(all_accounts)} accounts")

    # ── Transactions ─────────────────────────────────────────
    all_txns = []
    for acct in all_accounts:
        num_txns = random.randint(
            gen["transactions_per_account"]["min"],
            gen["transactions_per_account"]["max"],
        )
        running_balance = acct["balance"]
        for _ in range(num_txns):
            txn = generate_transaction(
                acct["account_id"], running_balance, cfg, branch_ids
            )
            all_txns.append(txn)
            if txn["status"] == "completed":
                running_balance = txn["balance_after"]

    logger.info(f"Generating {len(all_txns)} transactions...")

    # Insert in batches of 5000 to avoid memory issues
    batch_size = 5000
    for i in range(0, len(all_txns), batch_size):
        batch = all_txns[i : i + batch_size]
        execute_values(
            cur,
            """
            INSERT INTO transactions (
                transaction_id, account_id, transaction_type,
                amount, balance_before, balance_after, currency,
                status, channel, description, reference_id,
                counterparty_account, merchant_name, merchant_category,
                is_flagged, flag_reason, processed_branch_id, transaction_at
            ) VALUES %s
            """,
            [
                (
                    t["transaction_id"], t["account_id"], t["transaction_type"],
                    t["amount"], t["balance_before"], t["balance_after"],
                    t["currency"], t["status"], t["channel"], t["description"],
                    t["reference_id"], t["counterparty_account"],
                    t["merchant_name"], t["merchant_category"],
                    t["is_flagged"], t["flag_reason"],
                    t["processed_branch_id"], t["transaction_at"],
                )
                for t in batch
            ],
        )
        conn.commit()
        logger.info(f"    Batch {i // batch_size + 1}: inserted {len(batch)} txns")

    logger.info(f"  ✅ Inserted {len(all_txns)} transactions total")

    # ── Loans ────────────────────────────────────────────────
    # Loans go to loan-type accounts + a subset of other accounts
    loan_accounts = [a for a in all_accounts if a["account_type"] == "loan"]
    other_candidates = [a for a in all_accounts if a["account_type"] != "loan"]
    extra_count = min(
        int(num_customers * gen["loans_probability"]),
        len(other_candidates),
    )
    loan_accounts.extend(random.sample(other_candidates, k=extra_count))

    loans = []
    for acct in loan_accounts:
        loan = generate_loan(acct["account_id"], acct["customer_id"], cfg)
        loans.append(loan)

    if loans:
        logger.info(f"Generating {len(loans)} loans...")
        execute_values(
            cur,
            """
            INSERT INTO loans (
                loan_id, account_id, customer_id, loan_type,
                principal, interest_rate, term_months, monthly_payment,
                total_paid, outstanding, status, next_payment_date,
                disbursed_at, maturity_date
            ) VALUES %s
            """,
            [
                (
                    l["loan_id"], l["account_id"], l["customer_id"],
                    l["loan_type"], l["principal"], l["interest_rate"],
                    l["term_months"], l["monthly_payment"], l["total_paid"],
                    l["outstanding"], l["status"], l["next_payment_date"],
                    l["disbursed_at"], l["maturity_date"],
                )
                for l in loans
            ],
        )
        conn.commit()
        logger.info(f"  ✅ Inserted {len(loans)} loans")

    # ── Summary ──────────────────────────────────────────────
    logger.info("=" * 55)
    logger.info("SEED COMPLETE")
    logger.info(f"  Branches:     {len(branch_ids)} (pre-seeded)")
    logger.info(f"  Customers:    {num_customers}")
    logger.info(f"  Accounts:     {len(all_accounts)}")
    logger.info(f"  Transactions: {len(all_txns)}")
    logger.info(f"  Loans:        {len(loans)}")
    flagged = sum(1 for t in all_txns if t["is_flagged"])
    logger.info(f"  Flagged txns: {flagged}")
    logger.info("=" * 55)
    cur.close()


# ══════════════════════════════════════════════════════════════
# STREAM MODE
# ══════════════════════════════════════════════════════════════
def stream_data(conn, cfg: dict):
    gen = cfg["generation"]["streaming"]
    cur = conn.cursor()
    branch_ids = fetch_branch_ids(conn)

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
                """
                SELECT account_id, balance
                FROM accounts
                WHERE status = 'active'
                ORDER BY RANDOM()
                LIMIT %s
                """,
                (gen["batch_size"],),
            )
            accounts = cur.fetchall()

            for account_id, balance in accounts:
                if random.random() < gen["update_probability"]:
                    # UPDATE — triggers CDC update event
                    action = random.choice(["credit_score", "risk_rating", "status"])

                    if action == "credit_score":
                        new_score = random.randint(300, 850)
                        cur.execute(
                            """
                            UPDATE customers SET credit_score = %s
                            WHERE customer_id = (
                                SELECT customer_id FROM accounts
                                WHERE account_id = %s
                            )
                            """,
                            (new_score, str(account_id)),
                        )
                        logger.info(
                            f"  📝 Credit score → {new_score} "
                            f"(acct {str(account_id)[:8]}…)"
                        )

                    elif action == "risk_rating":
                        new_rating = weighted_choice(
                            cfg["distributions"]["risk_ratings"]
                        )
                        cur.execute(
                            """
                            UPDATE customers SET risk_rating = %s
                            WHERE customer_id = (
                                SELECT customer_id FROM accounts
                                WHERE account_id = %s
                            )
                            """,
                            (new_rating, str(account_id)),
                        )
                        logger.info(
                            f"  📝 Risk rating → {new_rating} "
                            f"(acct {str(account_id)[:8]}…)"
                        )

                    elif action == "status":
                        new_status = random.choice(["active", "frozen", "inactive"])
                        cur.execute(
                            "UPDATE accounts SET status = %s WHERE account_id = %s",
                            (new_status, str(account_id)),
                        )
                        logger.info(
                            f"  📝 Account status → {new_status} "
                            f"({str(account_id)[:8]}…)"
                        )

                else:
                    # INSERT — new transaction
                    txn = generate_transaction(
                        str(account_id),
                        Decimal(str(balance)),
                        cfg,
                        branch_ids,
                    )
                    cur.execute(
                        """
                        INSERT INTO transactions (
                            transaction_id, account_id, transaction_type,
                            amount, balance_before, balance_after, currency,
                            status, channel, description, reference_id,
                            counterparty_account, merchant_name,
                            merchant_category, is_flagged, flag_reason,
                            processed_branch_id, transaction_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            txn["transaction_id"], txn["account_id"],
                            txn["transaction_type"], txn["amount"],
                            txn["balance_before"], txn["balance_after"],
                            txn["currency"], txn["status"], txn["channel"],
                            txn["description"], txn["reference_id"],
                            txn["counterparty_account"], txn["merchant_name"],
                            txn["merchant_category"], txn["is_flagged"],
                            txn["flag_reason"], txn["processed_branch_id"],
                            txn["transaction_at"],
                        ),
                    )

                    # Update account balance on completed txns
                    if txn["status"] == "completed":
                        cur.execute(
                            "UPDATE accounts SET balance = %s WHERE account_id = %s",
                            (txn["balance_after"], str(account_id)),
                        )

                    flag_marker = " 🚩" if txn["is_flagged"] else ""
                    logger.info(
                        f"  💰 {txn['transaction_type']} "
                        f"${txn['amount']} on {str(account_id)[:8]}…{flag_marker}"
                    )

            # Occasionally add a new customer + account
            if random.random() < gen["new_customer_probability"]:
                cust = generate_customer(cfg, branch_ids)
                cur.execute(
                    """
                    INSERT INTO customers (
                        customer_id, first_name, last_name, email, phone,
                        date_of_birth, address, city, state, zip_code,
                        country, credit_score, annual_income,
                        employment_status, risk_rating, home_branch_id,
                        is_active, created_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (email) DO NOTHING
                    """,
                    (
                        cust["customer_id"], cust["first_name"],
                        cust["last_name"], cust["email"], cust["phone"],
                        cust["date_of_birth"], cust["address"], cust["city"],
                        cust["state"], cust["zip_code"], cust["country"],
                        cust["credit_score"], cust["annual_income"],
                        cust["employment_status"], cust["risk_rating"],
                        cust["home_branch_id"], cust["is_active"],
                        cust["created_at"],
                    ),
                )

                acct = generate_account(cust["customer_id"], cfg, branch_ids)
                cur.execute(
                    """
                    INSERT INTO accounts (
                        account_id, customer_id, account_type,
                        account_number, balance, currency, interest_rate,
                        credit_limit, overdraft_limit, status,
                        opened_branch_id, opened_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (account_number) DO NOTHING
                    """,
                    (
                        acct["account_id"], acct["customer_id"],
                        acct["account_type"], acct["account_number"],
                        acct["balance"], acct["currency"],
                        acct["interest_rate"], acct["credit_limit"],
                        acct["overdraft_limit"], acct["status"],
                        acct["opened_branch_id"], acct["opened_at"],
                    ),
                )
                logger.info(
                    f"  🆕 New customer: "
                    f"{cust['first_name']} {cust['last_name']} "
                    f"({cust['employment_status']}, risk={cust['risk_rating']})"
                )

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
        help="seed=bulk insert, stream=continuous CDC, both=seed then stream",
    )
    parser.add_argument(
        "--config", default="config.yaml", help="Path to config file"
    )
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
