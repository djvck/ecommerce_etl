import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

#--------------------------------------------------------------------
# Extract Phase
#--------------------------------------------------------------------

print("ETL-Pipeline started ....\n\n")

# - Orders
print("Extracting 'orders_raw.csv'")
df_orders = pd.read_csv("data/orders_raw.csv")
print(f"Extracted {len(df_orders)} data entries")
# - Customers
print("Extracting 'customers_raw.csv'")
df_customers = pd.read_csv("data/customers_raw.csv")
print(f"Extracted {len(df_customers)} data entries")
# - Products
print("Extracting 'products_raw.csv'")
df_prods = pd.read_csv("data/products_raw.csv")
print(f"Extracted {len(df_prods)} data entries")



# POSTGRES CONNECTION
# pip install psycopg2-binary


conn = psycopg2.connect(
    host ="localhost",
    dbname = "ecom",
    user = "nme",
    password = "secret"
)

cur = conn.cursor()


# Create Raw Tables
# Create Clean Tables


#--------------------------------------------------------------------
# LOAD RAW DATA INTO POSTGRES
#--------------------------------------------------------------------

print("INSERTING DATA INTO ORDERS_RAW")
records = df_orders.to_records(index=False)
values = list(df_orders.itertuples(index=False, name=None))

sql_orders = """
INSERT INTO orders_raw (order_id, user_id, product, price, quantity, order_date, country, email, status)
VALUES %s
ON CONFLICT (order_id) DO NOTHING
"""
execute_values(cur, sql_orders, values)
conn.commit()
print("ORDERS_RAW DONE")
print(f"Inserted rows: {len(values)}")


print("INSERTING DATA INTO CUSTOMERS_RAW")
records = df_customers.to_records(index=False)
values = list(df_customers.itertuples(index=False, name=None))

sql_customers = """
INSERT INTO customers_raw (user_id, first_name, last_name, email, country, gender, signup_date, is_active)
VALUES %s
ON CONFLICT (user_id) DO NOTHING
"""
execute_values(cur, sql_customers, values)
conn.commit()

print("CUSTOMERS_RAW DONE")
print(f"Inserted rows: {len(values)}")


print("\n\nETL-Pipeline done ....\n\n")