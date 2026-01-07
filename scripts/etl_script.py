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



# - This allows only unique entries, so no dupes
#ALTER TABLE orders_clean
#ADD CONSTRAINT orders_clean_order_id_unique UNIQUE (order_id);

# Method 2: TRUNCATE the table everytime before inserting data;

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
print(f"Inserted rows: {len(values)}")
print("CUSTOMERS_RAW DONE")
print("INSERTING DATA INTO PRODUCTS_RAW")
records = df_prods.to_records(index=False)
values = list(df_prods.itertuples(index=False, name=None))
sql_prods = """
INSERT INTO products_raw (product_id, product_name, category, brand, supplier, base_price, stock_quantity, rating, is_active )
VALUES %s
ON CONFLICT (product_id) DO NOTHING
"""
execute_values(cur, sql_prods, values)
conn.commit()
print(f"Inserted rows: {len(values)}")
print("PRODUCTS RAW DONE")





# - Transform Stage

# - Dropping Duplicate Values
df_customers.drop_duplicates(inplace=True)
df_orders.drop_duplicates(inplace=True)
df_prods.drop_duplicates(inplace=True)


# - Filling Not Critical Missing Values    
df_customers["email"] = df_customers["email"].fillna('Unknown')
df_customers["gender"] = df_customers["gender"].fillna('Unknown')
df_orders["email"] = df_orders["email"].fillna('Unknown')

# - Dropping Data with Critical Values missing 
# df.dropna(subset = ['column1', 'column2', 'column3'], inplace=True)

print("CUSTOMERS MISSING\n")
print(df_customers.isna().sum())
print("ORDERS MISSING\n")
print(df_orders.isna().sum())
print("PRODS MISSING\n")
print(df_prods.isna().sum())


# Making a List with column names who have mssing values
cols_with_nulls = df_orders.columns[df_orders.isnull().any()].tolist()
print(cols_with_nulls)

# 2 variant
#null_counts = df.isna().sum()
#cols_with_nulls = null_counts[null_counts > 0].index.tolist()


print("\n\nETL-Pipeline done ....\n\n")