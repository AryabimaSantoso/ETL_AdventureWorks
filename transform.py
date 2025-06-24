import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

STAGING_DSN = os.getenv(
    "STAGING_DSN",
    "postgresql+psycopg2://postgres:password@localhost:5432/staging"
)

engine = create_engine(STAGING_DSN)

def transform():
    # Load data dari staging
    soh = pd.read_sql("SELECT * FROM salesorderheader", engine, parse_dates=["orderdate"])
    sod = pd.read_sql("SELECT * FROM salesorderdetail", engine)
    customer = pd.read_sql("SELECT * FROM customer", engine)
    product = pd.read_sql("SELECT * FROM product", engine)
    salesperson = pd.read_sql("SELECT * FROM salesperson", engine)
    territory = pd.read_sql("SELECT * FROM territory", engine)
    creditcard = pd.read_sql("SELECT * FROM creditcard", engine)

    # dim_date
    dim_date = soh[["orderdate"]].drop_duplicates().dropna()
    dim_date = dim_date.rename(columns={"orderdate": "date"})
    dim_date["year"] = dim_date["date"].dt.year
    dim_date["month"] = dim_date["date"].dt.month
    dim_date["day"] = dim_date["date"].dt.day
    dim_date["quarter"] = dim_date["date"].dt.quarter
    dim_date.to_sql("dim_date", engine, if_exists='replace', index=False)

    # dim_customer
    dim_customer = customer[["customerid", "personid", "storeid", "territoryid", "modifieddate"]]
    dim_customer.to_sql("dim_customer", engine, if_exists='replace', index=False)

    # dim_product
    dim_product = product[[
        "productid", "productnumber", "color", "size", "weight",
        "standardcost", "listprice", "productline", "class", "style",
        "sellstartdate", "sellenddate", "rowguid", "modifieddate"
    ]]
    dim_product.to_sql("dim_product", engine, if_exists='replace', index=False)

    # dim_salesperson
    dim_salesperson = salesperson[[
        "businessentityid", "salesquota", "bonus", "commissionpct",
        "salesytd", "saleslastyear", "rowguid", "modifieddate"
    ]].rename(columns={"businessentityid": "salespersonid"})
    dim_salesperson.to_sql("dim_salesperson", engine, if_exists='replace', index=False)

    # dim_territory
    dim_territory = territory[[
        "territoryid", "countryregioncode", "region_group",
        "salesytd", "saleslastyear", "costytd", "costlastyear",
        "rowguid", "modifieddate"
    ]]
    dim_territory.to_sql("dim_territory", engine, if_exists='replace', index=False)

    # dim_creditcard
    dim_creditcard = creditcard[[
        "creditcardid", "cardtype", "cardnumber", "expmonth", "expyear", "modifieddate"
    ]]
    dim_creditcard.to_sql("dim_creditcard", engine, if_exists='replace', index=False)

    # fact_sales
    fact_sales = soh[[
        "salesorderid", "customerid", "salespersonid", "territoryid",
        "creditcardid", "subtotal", "taxamt", "freight", "totaldue", "orderdate"
    ]].merge(
        sod[["salesorderid", "productid", "unitprice", "unitpricediscount"]],
        on="salesorderid"
    )
    fact_sales.to_sql("fact_sales", engine, if_exists='replace', index=False)

    print("✅ Transformasi selesai dan hasil disimpan ke staging.")

if __name__ == "__main__":
    transform()






