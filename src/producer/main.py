from utility.session_manager.spark_session import SparkSessionFactory
from utility.path_manager import PathBuilder
from faker import Faker
import shutil
import random
from datetime import date

import pyspark.sql.types as T


def maybe_null(value, null_probability=0.05):
    return value if random.random() > null_probability else None


def write_single_csv(spark, data, schema, output_dir, filename):
    temp_dir = output_dir / "temp"
    df = spark.createDataFrame(data, schema=schema)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(temp_dir))

    for file in temp_dir.glob("*.csv"):
        file.rename(output_dir / filename)

    shutil.rmtree(temp_dir)


if __name__ == "__main__":
    spark = SparkSessionFactory().get_or_create_spark_session()
    fake = Faker()

    # ✅ Use PathBuilder to get root and point to /out directory
    project_root = PathBuilder().get_root_directory()
    out_dir = project_root / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    ### 1. CUSTOMER
    customer_schema = T.StructType([
        T.StructField("Customer Id", T.StringType(), True),
        T.StructField("Full Name", T.StringType(), True),
        T.StructField("Last Name", T.StringType(), True),
        T.StructField("Email", T.StringType(), True),
        T.StructField("Phone Number", T.StringType(), True),
        T.StructField("Date of Birth", T.DateType(), True),
        T.StructField("Gender", T.StringType(), True),
        T.StructField("City", T.StringType(), True),
        T.StructField("State", T.StringType(), True),
        T.StructField("Postal Code", T.StringType(), True),
        T.StructField("Country", T.StringType(), True),
        T.StructField("Registration Date", T.DateType(), True)
    ])
    customer_data = []
    for _ in range(1000):
        dob = fake.date_of_birth(minimum_age=18, maximum_age=70)
        registration_date = fake.date_between(start_date=dob, end_date=date.today())
        customer_data.append((
            fake.uuid4(),
            maybe_null(fake.name(), 0.05),
            maybe_null(fake.last_name(), 0.2),
            maybe_null(fake.email(), 0.1),
            fake.phone_number(),
            dob,
            maybe_null(random.choice(["Male", "Female", "Other"]), 0.05),
            maybe_null(fake.city(), 0.05),
            maybe_null(fake.state(), 0.05),
            maybe_null(fake.postcode(), 0.05),
            maybe_null(fake.country(), 0.05),
            registration_date
        ))
    write_single_csv(spark, customer_data, customer_schema, out_dir, "customer.csv")

    ### 2. STORE
    store_schema = T.StructType([
        T.StructField("Store Id", T.StringType(), True),
        T.StructField("Store Name", T.StringType(), True),
        T.StructField("City", T.StringType(), True),
        T.StructField("State", T.StringType(), True),
        T.StructField("Postal Code", T.StringType(), True),
        T.StructField("Country", T.StringType(), True),
        T.StructField("Opened Date", T.DateType(), True),
        T.StructField("Store Type", T.StringType(), True),
    ])
    store_data = []
    for _ in range(50):
        store_data.append((
            fake.uuid4(),
            fake.company(),
            fake.city(),
            fake.state(),
            fake.postcode(),
            fake.country(),
            fake.date_between(start_date="-10y", end_date="today"),
            random.choice(["Flagship", "Franchise", "Kiosk"])
        ))
    write_single_csv(spark, store_data, store_schema, out_dir, "store.csv")

    ### 3. PRODUCT
    product_schema = T.StructType([
        T.StructField("Product Id", T.StringType(), True),
        T.StructField("Product Name", T.StringType(), True),
        T.StructField("Product Subclass", T.StringType(), True),
        T.StructField("Product Class", T.StringType(), True),
        T.StructField("Department", T.StringType(), True),
        T.StructField("Product Group", T.StringType(), True),
        T.StructField("Division", T.StringType(), True),
    ])
    product_data = []
    for _ in range(200):
        product_data.append((
            fake.uuid4(),
            fake.word().title(),
            random.choice(["Men's Shirts", "Women's Jeans", "Kids' Jackets"]),
            random.choice(["Shirts", "Jeans", "Jackets"]),
            random.choice(["Apparel", "Footwear"]),
            random.choice(["Fashion", "Athletic"]),
            random.choice(["Retail", "Outlet"])
        ))
    write_single_csv(spark, product_data, product_schema, out_dir, "product.csv")

    ### 4. SALES CHANNEL
    sales_channel_schema = T.StructType([
        T.StructField("Channel Id", T.StringType(), True),
        T.StructField("Channel Name", T.StringType(), True),
        T.StructField("Channel Type", T.StringType(), True),
        T.StructField("Region", T.StringType(), True),
    ])
    sales_channel_data = []
    for _ in range(10):
        sales_channel_data.append((
            fake.uuid4(),
            random.choice(["Online", "In-store", "Call Center"]),
            random.choice(["Direct", "Third-party"]),
            random.choice(["APAC", "EMEA", "NA", "LATAM"])
        ))
    write_single_csv(spark, sales_channel_data, sales_channel_schema, out_dir, "sales_channel.csv")

    ### 5. PAYMENT METHOD
    payment_method_schema = T.StructType([
        T.StructField("Payment Method Id", T.StringType(), True),
        T.StructField("Payment Method Name", T.StringType(), True),
        T.StructField("Provider", T.StringType(), True),
        T.StructField("Category", T.StringType(), True),
        T.StructField("Is Digital", T.BooleanType(), True)
    ])
    payment_method_data = []
    for _ in range(15):
        category = random.choice(["Card", "Wallet", "Bank Transfer", "Cash"])
        payment_method_data.append((
            fake.uuid4(),
            category + " - " + fake.word().title(),
            random.choice(["Visa", "PayPal", "Stripe", "Bank XYZ"]),
            category,
            category != "Cash"
        ))
    write_single_csv(spark, payment_method_data, payment_method_schema, out_dir, "payment_method.csv")

    spark.stop()
