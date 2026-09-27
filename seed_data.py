from datetime import date, timedelta
from decimal import Decimal
import random

import psycopg


CUSTOMER_COUNT = 1_000
ACCOUNT_COUNT = 1_500
TRANSACTION_COUNT = 10_000
BATCH_SIZE = 1_000


def insert_in_batches(cur, sql, rows):
    for start in range(0, len(rows), BATCH_SIZE):
        cur.executemany(sql, rows[start:start + BATCH_SIZE])


def main():
    rng = random.Random(42)

    # PGHOST، PGPORT، PGDATABASE، PGUSER و PGPASSWORD
    # از متغیرهای محیطی خوانده می‌شوند.
    with psycopg.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            database = cur.fetchone()[0]

            if database != "iceberg_lab":
                raise RuntimeError(
                    f"Wrong database: {database}. Expected iceberg_lab."
                )

            # ----------------------------------------------------------
            # 1. Customers: 1000 customers
            # ----------------------------------------------------------
            first_names = [
                "Anna", "Max", "Sara", "Lukas", "Emma",
                "Paul", "Lea", "Jonas", "Mia", "Ben",
            ]
            last_names = [
                "Müller", "Schneider", "Weber", "Fischer", "Wagner",
                "Becker", "Hoffmann", "Schulz", "Koch", "Richter",
            ]

            customers = []
            for i in range(1, CUSTOMER_COUNT + 1):
                customer_id = f"C{i:03d}"

                if i == 1:
                    first_name, last_name = "Anna", "Müller"
                elif i == 2:
                    first_name, last_name = "Max", "Schneider"
                elif i == 3:
                    first_name, last_name = "Sara", "Weber"
                else:
                    first_name = first_names[(i - 1) % len(first_names)]
                    last_name = last_names[((i - 1) // len(first_names)) % len(last_names)]

                customers.append((customer_id, first_name, last_name))

            insert_in_batches(cur, """
                INSERT INTO source.customers
                    (customer_id, first_name, last_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (customer_id) DO NOTHING
            """, customers)

            # ----------------------------------------------------------
            # 2. Accounts: 1500 accounts
            # ----------------------------------------------------------
            original_accounts = {
                1: ("C001", date(2024, 1, 15)),
                2: ("C001", date(2025, 3, 10)),
                3: ("C002", date(2023, 6, 1)),
                4: ("C003", date(2025, 8, 20)),
            }

            accounts = []
            account_owners = {}

            for i in range(1, ACCOUNT_COUNT + 1):
                account_id = f"A{i:03d}"

                if i in original_accounts:
                    customer_id, opened_at = original_accounts[i]
                else:
                    customer_number = ((i - 1) % CUSTOMER_COUNT) + 1
                    customer_id = f"C{customer_number:03d}"
                    opened_at = date(2023, 1, 1) + timedelta(days=i % 1000)

                account_owners[account_id] = customer_id

                accounts.append((
                    account_id,
                    customer_id,
                    "EUR",
                    "ACTIVE",
                    opened_at,
                ))

            insert_in_batches(cur, """
                INSERT INTO source.accounts
                    (account_id, customer_id, currency, status, opened_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (account_id) DO NOTHING
            """, accounts)

            # ----------------------------------------------------------
            # 3. GL accounts: 30 accounting account definitions
            # ----------------------------------------------------------
            gl_accounts = [
                ("1000", "Bank settlement account", "ASSET"),
                ("1200", "Customer loan receivables", "ASSET"),
                ("2000", "Customer deposits", "LIABILITY"),
                ("4000", "Interest income", "INCOME"),
                ("4100", "Fee income", "INCOME"),
            ]

            for i in range(1, 26):
                gl_accounts.append((
                    str(5000 + i),
                    f"Operating expense category {i:02d}",
                    "EXPENSE",
                ))

            insert_in_batches(cur, """
                INSERT INTO source.gl_accounts
                    (gl_account_code, account_name, account_category)
                VALUES (%s, %s, %s)
                ON CONFLICT (gl_account_code) DO NOTHING
            """, gl_accounts)

            # ----------------------------------------------------------
            # 4. Transactions: 10000 transactions
            # ----------------------------------------------------------
            transaction_types = [
                "TRANSFER",
                "CARD_PAYMENT",
                "LOAN_PAYMENT",
            ]

            # 12 رکورد اولیه با همان مشخصات اسکریپت قبلی.
            transactions = []
            original_account_ids = ["A001", "A002", "A003", "A004"]

            for i in range(1, 13):
                account_id = original_account_ids[(i - 1) % 4]

                transactions.append((
                    f"T{i:04d}",
                    account_owners[account_id],
                    account_id,
                    transaction_types[(i - 1) % len(transaction_types)],
                    Decimal("25.00") + Decimal(i * 10),
                    "EUR",
                    "BOOKED",
                    date(2026, 9, 1) + timedelta(days=(i - 1) % 10),
                ))

            insert_sql = """
                INSERT INTO source.transactions
                    (transaction_id, customer_id, account_id,
                     transaction_type, amount, currency, status, booking_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
            """

            insert_in_batches(cur, insert_sql, transactions)

            account_ids = list(account_owners)
            batch = []

            for i in range(13, TRANSACTION_COUNT + 1):
                account_id = account_ids[rng.randrange(len(account_ids))]
                customer_id = account_owners[account_id]

                # پخش تاریخ تراکنش‌ها در ۱ تا ۲۴ سپتامبر ۲۰۲۶
                booking_date = date(2026, 9, 1) + timedelta(
                    days=rng.randrange(24)
                )

                # مبلغ 10.00 تا 2000.00 یورو
                amount = Decimal(rng.randint(1000, 200000)) / Decimal(100)

                batch.append((
                    f"T{i:04d}",
                    customer_id,
                    account_id,
                    transaction_types[rng.randrange(len(transaction_types))],
                    amount,
                    "EUR",
                    "BOOKED",
                    booking_date,
                ))

                if len(batch) == BATCH_SIZE:
                    cur.executemany(insert_sql, batch)
                    batch.clear()

            if batch:
                cur.executemany(insert_sql, batch)

            # ----------------------------------------------------------
            # Result
            # ----------------------------------------------------------
            for table in (
                "customers",
                "accounts",
                "transactions",
                "gl_accounts",
            ):
                cur.execute(f"SELECT count(*) FROM source.{table}")
                print(f"{table}: {cur.fetchone()[0]} rows")


if __name__ == "__main__":
    main()