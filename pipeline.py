from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql.functions import regexp_replace, col, to_date
import psycopg2

# URL of the CSV file
url = 'https://catalog.data.gov/dataset/real-estate-sales-2001-2018/resource/f7cb94d8-283c-476f-a966-cc8c9e1308b4'
schema_name = 'public'  # Define your schema name here

# Scraping CSV file from website
def get_soup(url):
    return bs(requests.get(url).text, 'html.parser')

def get_unique_csv_links(url, data_format="csv"):
    unique_links = set()
    for link in get_soup(url).find_all('a'):
        file_link = link.get('href')
        if file_link and data_format in file_link:
            unique_links.add(file_link)
    return unique_links

def load_csv_to_pandas(csv_url):
    columns = [
        'Serial Number',
        'List Year',
        'Date Recorded',
        'Town',
        'Address',
        'Assessed Value',
        'Sale Amount',
        'Sales Ratio',
        'Property Type',
        'Residential Type',
        'Location'
    ]
    df = pd.read_csv(csv_url, usecols=columns)
    return df

def load_csv_to_spark(pandas_df):
    spark = SparkSession.builder \
        .appName("CSV to Spark") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    schema = StructType([
        StructField('Serial Number', IntegerType(), True),
        StructField('List Year', IntegerType(), True),
        StructField('Date Recorded', StringType(), True),
        StructField('Town', StringType(), True),
        StructField('Address', StringType(), True),
        StructField('Assessed Value', FloatType(), True),
        StructField('Sale Amount', FloatType(), True),
        StructField('Sales Ratio', FloatType(), True),
        StructField('Property Type', StringType(), True),
        StructField('Residential Type', StringType(), True),
        StructField('Location', StringType(), True)
    ])

    spark_df = spark.createDataFrame(pandas_df, schema=schema)

    renamed_df = spark_df \
        .withColumnRenamed('Serial Number', 'SN') \
        .withColumnRenamed('List Year', 'list_year') \
        .withColumnRenamed('Date Recorded', 'date_recorded') \
        .withColumnRenamed('Town', 'town') \
        .withColumnRenamed('Address', 'address') \
        .withColumnRenamed('Assessed Value', 'assessed_value') \
        .withColumnRenamed('Sale Amount', 'sale_amount') \
        .withColumnRenamed('Sales Ratio', 'sales_ratio') \
        .withColumnRenamed('Property Type', 'property_type') \
        .withColumnRenamed('Residential Type', 'residential_type') \
        .withColumnRenamed('Location', 'location')

    renamed_df = renamed_df.repartition(100)
    return renamed_df

def clean_data(spark_df):
    spark_df = spark_df.fillna('')
    spark_df = spark_df.withColumn("sales_ratio", regexp_replace(col("sales_ratio"), '[^\d.]', '').cast(FloatType())).filter(col("sales_ratio") >= 0)
    spark_df = spark_df.withColumn("sale_amount", regexp_replace(col("sale_amount"), '[^\d.]', '').cast(FloatType())).filter(col("sale_amount") >= 0)
    spark_df = spark_df.withColumn("date_recorded", to_date(col("date_recorded"), "MM/dd/yyyy"))
    spark_df = spark_df.withColumn("list_year", col("list_year").cast(IntegerType()))
    spark_df = spark_df.dropDuplicates(["SN"])
    return spark_df

def create_table_if_not_exists(connection, cursor):
    try:
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {schema_name}.border_crossing_data (
                SN VARCHAR(255),
                list_year INTEGER,
                date_recorded DATE,
                town VARCHAR(255),
                address VARCHAR(255),
                assessed_value FLOAT,
                sale_amount FLOAT,
                sales_ratio FLOAT,
                property_type VARCHAR(255),
                residential_type VARCHAR(255),
                location VARCHAR(255)
            )
        ''')
        print("Table created or already exists!")
    except Exception as e:
        print(f"Table creation failed: {e}")

def write_to_postgres(spark_df, connection, cursor):
    insert_query = f'''
        INSERT INTO {schema_name}.border_crossing_data (
            SN, list_year, date_recorded, town, address, assessed_value, sale_amount, sales_ratio, property_type, residential_type, location
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    try:
        for row in spark_df.collect():
            cursor.execute(insert_query, (
                row['SN'], row['list_year'], row['date_recorded'], row['town'], row['address'],
                row['assessed_value'], row['sale_amount'], row['sales_ratio'], row['property_type'],
                row['residential_type'], row['location']
            ))
        connection.commit()
        print("Data inserted successfully")
    except Exception as e:
        print(f"Data insertion failed: {e}")

def main():
    # Create database connection
    try:
        connection = psycopg2.connect(
            host="192.168.0.2",
            user="prasis_angdembe",
            password="xyz",
            database="hello",
            port=5678
        )
        cursor = connection.cursor()
        print("Database connection successful!")
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return

    # Scrape and process CSV
    unique_links = get_unique_csv_links(url)
    print("Unique CSV links found on the page:")
    for link in unique_links:
        print(link)
    
    # Taking the first CSV link
    csv_url = list(unique_links)[0]
    
    # Load csv into pandas df
    pandas_df = load_csv_to_pandas(csv_url)

    # Load pandas df into Spark df
    spark_df = load_csv_to_spark(pandas_df)

    # Clean data
    spark_df = clean_data(spark_df)
    print("Cleaned Spark DataFrame:")
    spark_df.show()

    # Create table if it does not exist
    create_table_if_not_exists(connection, cursor)

    # Write data to PostgreSQL
    write_to_postgres(spark_df, connection, cursor)

    # Close connection
    cursor.close()
    connection.close()

if __name__ == "__main__":
    main()
