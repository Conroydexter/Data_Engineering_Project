from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
import os
import psycopg2
from dotenv import load_dotenv
import dbstatus  # Import the dbstatus module

# Load environment variables from .env file
load_dotenv()

# Accessing PostgreSQL environment variables
db_name = os.getenv('POSTGRES_DB')
db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')

# Initialize Spark session
spark = SparkSession.builder \
    .appName('Data_Engineering_Project') \
    .config("spark.jars", "C:\\Users\\admin\\PycharmProjects\\Data_Engineering_Project\\code\\libs\\postgresql-42.7.4.jar") \
    .getOrCreate()

def create_table():
    """Create the necessary tables for the ETL process."""
    print("Creating tables...", flush=True)
    connection = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
    cursor = connection.cursor()

    # SQL commands to create the tables
    sql = """
        CREATE TABLE IF NOT EXISTS admission (
            "Medical Condition" VARCHAR(60),
            "Hospital" VARCHAR(60),
            "Insurance Provider" VARCHAR(60),
            "Average Bill Amount" NUMERIC(7, 2),
            "Admission Type" VARCHAR(60),
            "Quarter" INTEGER,  
            "Discharge Date" DATE
        );

        CREATE TABLE IF NOT EXISTS status (
            id SERIAL PRIMARY KEY,
            status INT,
            message VARCHAR(40),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            lastloaded DATE
        );
    """
    try:
        cursor.execute(sql)
        connection.commit()
        print("Tables created successfully.", flush=True)
    except Exception as e:
        print(f"Error creating tables: {e}", flush=True)
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

def get_last_date_loaded():
    """Retrieve the last loaded date and clean incomplete loads."""
    connection = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
    cursor = connection.cursor()

    cursor.execute("SELECT MAX(lastloaded) as lastloaded FROM status;")
    last_loaded = cursor.fetchone()[0]
    print(f"Last loaded date from status: {last_loaded}", flush=True)

    if last_loaded is None:
        last_loaded = '2019-05-08'
        print("No last loaded date found. Resetting to default date.", flush=True)
        cursor.execute("DELETE FROM admission;")
        connection.commit()
        print("Cleared admission table.", flush=True)

    cursor.close()
    connection.close()
    print(f"Returning last loaded date: {last_loaded}", flush=True)
    return last_loaded

def extract():
    """Extract data from the healthcare_dataset.csv file."""
    print("ETL 1/7 - Extraction started", flush=True)
    dbstatus.logStatus(2, "Extraction started")  # Log extraction start

    # Define schema for the data
    schema = StructType([
        StructField("Name", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("Gender", StringType(), True),
        StructField("Blood Type", StringType(), True),
        StructField("Medical Condition", StringType(), True),
        StructField("Date of Admission", DateType(), True),
        StructField("Doctor", StringType(), True),
        StructField("Hospital", StringType(), True),
        StructField("Insurance Provider", StringType(), True),
        StructField("Billing Amount", DecimalType(precision=7, scale=2), True),
        StructField("Room Number", IntegerType(), True),
        StructField("Admission Type", StringType(), True),
        StructField("Discharge Date", DateType(), True),
        StructField("Medication", StringType(), True),
        StructField("Test Results", StringType(), True)
    ])

    try:
        data = spark.read.csv(r'C:\Users\admin\PycharmProjects\Data_Engineering_Project\data\healthcare_dataset.csv',
                              header=True, schema=schema)

        if data is not None and data.count() > 0:
            print("ETL 2/7 - Extraction completed successfully.", flush=True)
            dbstatus.logStatus(2, "Extraction completed successfully")  # Log success
            return data
        else:
            print("ETL extraction failed: No data found.", flush=True)
            dbstatus.logStatus(2, "Extraction failed: No data found")  # Log failure
            return None
    except Exception as e:
        print(f"ETL extraction failed: {e}", flush=True)
        dbstatus.logStatus(2, f"Extraction failed: {e}")  # Log exception
        return None

def transform(data):
    """Transform the extracted data."""
    print("ETL 3/7 - Transformation started", flush=True)
    dbstatus.logStatus(2, "Transformation started")  # Log transformation start

    # Remove unnecessary columns
    try:
        data = data.drop('Name', 'Age', 'Gender', 'Blood Type',
                         'Date of Admission', 'Doctor', 'Room Number',
                         'Medication', 'Test Results')
        print("Unnecessary columns removed successfully.", flush=True)
    except Exception as e:
        print(f"Failed to remove columns: {e}", flush=True)

    # Calculate average billing amount
    try:
        data = data.groupBy("Medical Condition", "Hospital",
                            "Insurance Provider", "Admission Type",
                            "Discharge Date").agg(F.avg("Billing Amount").alias("Average Bill Amount"))
        print("Average monthly billing amount calculated successfully.", flush=True)
    except Exception as e:
        print(f"Failed to calculate average billing amount: {e}", flush=True)

    # Add Quarter column
    try:
        data = data.withColumn("Quarter", ((F.month(data["Discharge Date"]) - 1) / 3).cast(IntegerType()) + 1)
        print("Quarter column added successfully.", flush=True)
    except Exception as e:
        print(f"Failed to add Quarter column: {e}", flush=True)

    # Sort data by Discharge Date
    try:
        data = data.orderBy("Discharge Date")
        print("Data sorted by Discharge Date successfully.", flush=True)
    except Exception as e:
        print(f"Failed to sort data: {e}", flush=True)

    print("ETL 4/7 - Transformation completed", flush=True)
    dbstatus.logStatus(2, "Transformation completed successfully")  # Log success
    return data

def load(data):
    """Load the transformed data into the database."""
    print("ETL 5/7 - Loading started", flush=True)
    dbstatus.logStatus(2, "Loading started")  # Log loading start

    last_loaded = str(get_last_date_loaded())
    last_loaded = datetime.strptime(last_loaded, '%Y-%m-%d') + relativedelta(months=1)

    # Filter the data based on discharge date
    data = data.filter(F.col("Discharge Date") >= last_loaded)

    connection = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
    cursor = connection.cursor()
    load_counter = 0

    for row in data.collect():
        row_dict = row.asDict()
        print(f"Available keys: {row_dict.keys()}", flush=True)

        try:
            average_bill_amount = float(row_dict["Average Bill Amount"])
        except KeyError as e:
            print(f"KeyError: {e} - Available keys: {row_dict.keys()}", flush=True)
            continue

        sql = (
            "INSERT INTO admission (\"Medical Condition\", \"Hospital\", \"Insurance Provider\", "
            "\"Average Bill Amount\", \"Admission Type\", \"Quarter\", \"Discharge Date\") "
            "VALUES ('%s', '%s', '%s', %.2f, '%s', '%s', '%s');"
        ) % (
            row_dict["Medical Condition"],
            row_dict["Hospital"],
            row_dict["Insurance Provider"],
            average_bill_amount,
            row_dict["Admission Type"],
            row_dict["Quarter"],
            row_dict["Discharge Date"]
        )

        print(f"Executing SQL: {sql}", flush=True)

        try:
            cursor.execute(sql)
            connection.commit()
            load_counter += 1
            print(f"Successfully loaded row: {row_dict['Medical Condition']} - {row_dict['Hospital']}", flush=True)
        except Exception as e:
            connection.rollback()
            print(f"Failed to load row: {row_dict['Medical Condition']} - {row_dict['Hospital']}. Error: {e}",
                  flush=True)

    cursor.close()
    connection.close()
    print("ETL 6/7 - Loading completed", flush=True)
    dbstatus.logStatus(2, f"Loading completed, {load_counter} rows loaded")  # Log loading completion
    print(f"ETL 7/7 - Loaded {load_counter} rows", flush=True)

    return "--- Process successfully completed! ---"

if __name__ == "__main__":
    # Run the ETL process
    create_table()
    last_loaded_date = get_last_date_loaded()
    extracted_data = extract()
    if extracted_data is not None:
        transformed_data = transform(extracted_data)
        load(transformed_data)
