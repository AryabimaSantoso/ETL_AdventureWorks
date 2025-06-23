import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Koneksi ke database sumber OLTP
SOURCE_DSN = os.getenv(
    "SOURCE_DSN",
    "postgresql+psycopg2://postgres:Mignon210905@localhost:5432/adventureworks"
)

# Koneksi ke database staging PostgreSQL
STAGING_DSN = os.getenv(
    "STAGING_DSN",
    "postgresql+psycopg2://postgres:Mignon210905@localhost:5432/staging"
)

engine_src = create_engine(SOURCE_DSN)
engine_staging = create_engine(STAGING_DSN)

# List tabel & query dari OLTP
queries = {
    # fact_sales
    "salesorderheader": """
        SELECT SalesOrderID, CustomerID, SalesPersonID, TerritoryID, CreditCardID, SubTotal, TaxAmt, Freight, TotalDue, OrderDate
        FROM Sales.SalesOrderHeader
    """,
    "salesorderdetail": """
        SELECT SalesOrderID, ProductID, UnitPrice, UnitPriceDiscount
        FROM Sales.SalesOrderDetail
    """,

    # dim_customer
    "customer": """
        SELECT CustomerID, PersonID, StoreID, TerritoryID, ModifiedDate
        FROM Sales.Customer
    """,

    # dim_product
    "product": """
    SELECT productid, productnumber, color, size, weight, standardcost, listprice, productline, class, style, sellstartdate, sellenddate, rowguid, modifieddate
    FROM production.product
    """,

    # dim_salesperson
    "salesperson": """
        SELECT BusinessEntityID, SalesQuota, Bonus, CommissionPct, SalesYTD, SalesLastYear, rowguid, ModifiedDate
        FROM Sales.SalesPerson
    """,

    # dim_territory
    "territory": """
        SELECT territoryid, countryregioncode, "group" AS region_group, salesytd, saleslastyear, costytd, costlastyear, rowguid, modifieddate
    FROM sales.salesterritory
    """,

    # dim_creditcard
    "creditcard": """
        SELECT CreditCardID, CardType, CardNumber, ExpMonth, ExpYear, ModifiedDate
        FROM Sales.CreditCard
    """
}


def extract_and_load():
    for table_name, sql in queries.items():
        print(f"[EXTRACT] {table_name}")
        df = pd.read_sql(text(sql), engine_src)
        df.to_sql(table_name, engine_staging, if_exists='replace', index=False, method='multi')
        print(f"→ {table_name} inserted to staging ({len(df)} rows)")

if __name__ == "__main__":
    extract_and_load()





