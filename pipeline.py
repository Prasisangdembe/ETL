from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import regexp_replace, col
import psycopg2

url = 'https://catalog.data.gov/dataset/border-crossing-entry-data-683ae/resource/46b04e29-f1fe-406b-8488-774367f9a549'
data_format = "csv"

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
    # Load CSV into Pandas DataFrame
    df = pd.read_csv(csv_url,header=50)
    return df

def load_csv_to_spark(pandas_df):
    spark = SparkSession.builder \
        .appName("CSV to Spark") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    schema = StructType([
        StructField('Port Name', StringType(), True),
        StructField('State', StringType(), True),
        StructField('Port Code', IntegerType(), True),
        StructField('Border', StringType(), True),
        StructField('Date', StringType(), True),
        StructField('Measure', StringType(), True),
        StructField('Value', IntegerType(), True),
        StructField('Latitude', DoubleType(), True),
        StructField('Longitude', DoubleType(), True),
        StructField('Point', StringType(), True)
    ])

    # Create Spark DataFrame from Pandas DataFrame
    spark_df = spark.createDataFrame(pandas_df, schema=schema)
    spark_df = spark_df.repartition(100) 
    return spark_df

# Data Cleaning
def clean_data(spark_df):
    # Replace null values with empty strings
    spark_df = spark_df.fillna('')
    # numeric columns do not contain alphabets
    spark_df = spark_df.withColumn("Value", regexp_replace(col("Value"), '[^0-9]', '').cast(IntegerType()))
    return spark_df

def load_to_mysql(spark_df):
    #connection setup
    try:
        connection = psycopg2.connect(
            host="172.31.17.197",
            user="prasis_angdembe",
            password="",
            database="",
            port=5678
        )
        print("Database connection successful!")
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return

    cursor = connection.cursor()

    # Creating table
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS public.border_crossing_data (
                port_name VARCHAR(255),
                state VARCHAR(255),
                port_code VARCHAR(255),
                border VARCHAR(255),
                date VARCHAR(255),
                measure VARCHAR(255),
                value VARCHAR(255),
                latitude VARCHAR(255),
                longitude VARCHAR(255),
                point VARCHAR(255)
            )
        ''')
        print("Table created or already exists!")
    except Exception as e:
        print(f"Table creation failed: {e}")
        return

    # Insert data into the table row-by-row
    try:
        for row in spark_df.collect():
            cursor.execute('''
                INSERT INTO public.border_crossing_data (port_name, state, port_code, border, date, measure, value, latitude, longitude, point)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', 
                (row['Port Name'], row['State'], row['Port Code'], row['Border'], row['Date'],
                row['Measure'], row['Value'], row['Latitude'], row['Longitude'], row['Point'])
                )
        connection.commit()
        print("Data inserted successfully")
    except Exception as e:
        print(f"Data insertion failed: {e}")
    finally:
        cursor.close()
        connection.close()

def main():
    unique_links = get_unique_csv_links(url)
    # Print the unique CSV links
    print("Unique CSV links found on the page")
    for link in unique_links:
        print(link)
    
    # Taking the first CSV link
    csv_url = list(unique_links)[0]
    
    # Load csv into pandas df
    pandas_df = load_csv_to_pandas(csv_url)

    #load pd df into spark df
    spark_df = load_csv_to_spark(pandas_df)
    #for data cleaning
    spark_df = clean_data(spark_df)
    print("Cleaned Spark DataFrame:")
    spark_df.show()

    #loading data into database
    load_to_mysql(spark_df)

if __name__ == "__main__":
    main()
