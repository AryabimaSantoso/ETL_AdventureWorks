import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

STAGING_DSN = os.getenv("STAGING_DSN", "postgresql+psycopg2://postgres:Mignon210905@localhost:5432/staging")
WAREHOUSE_DSN = os.getenv("WAREHOUSE_DSN", "postgresql+psycopg2://postgres:Mignon210905@localhost:5432/dw_adventureworks")

engine_staging = create_engine(STAGING_DSN)
engine_dw = create_engine(WAREHOUSE_DSN)

tables = [
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_salesperson",
    "dim_territory",
    "dim_creditcard",
    "fact_sales"
]

def create_foreign_keys():
    """Tambahkan foreign keys ke fact_sales jika belum ada"""
    fk_statements = [
        """
        ALTER TABLE fact_sales
        ADD CONSTRAINT fk_customer
        FOREIGN KEY (customerid) REFERENCES dim_customer(customerid)
        """,
        """
        ALTER TABLE fact_sales
        ADD CONSTRAINT fk_product
        FOREIGN KEY (productid) REFERENCES dim_product(productid)
        """,
        """
        ALTER TABLE fact_sales
        ADD CONSTRAINT fk_salesperson
        FOREIGN KEY (salespersonid) REFERENCES dim_salesperson(salespersonid)
        """,
        """
        ALTER TABLE fact_sales
        ADD CONSTRAINT fk_territory
        FOREIGN KEY (territoryid) REFERENCES dim_territory(territoryid)
        """,
        """
        ALTER TABLE fact_sales
        ADD CONSTRAINT fk_creditcard
        FOREIGN KEY (creditcardid) REFERENCES dim_creditcard(creditcardid)
        """,
        """
        ALTER TABLE fact_sales
        ADD CONSTRAINT fk_date
        FOREIGN KEY (orderdate) REFERENCES dim_date(date)
        """
    ]
    with engine_dw.begin() as conn:
        for stmt in fk_statements:
            try:
                conn.execute(text(stmt))
                print(f"✅ FK ditambahkan: {stmt.strip().splitlines()[0]}")
            except Exception as e:
                print(f"⚠️  FK sudah ada atau gagal: {e}")

def load_to_warehouse():
    for table in tables:
        print(f"[LOAD] {table}")
        df = pd.read_sql_table(table, engine_staging)

        with engine_dw.begin() as conn:
            conn.exec_driver_sql(f'TRUNCATE TABLE {table} RESTART IDENTITY CASCADE')

        df.to_sql(table, engine_dw, if_exists='append', index=False, method='multi')
        print(f"✅ {table} loaded: {len(df)} rows")

    create_foreign_keys()

if __name__ == "__main__":
    load_to_warehouse()






