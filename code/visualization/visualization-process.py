import decimal
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from bokeh.io import output_file, save
from bokeh.models import ColumnDataSource, DataTable, TableColumn
from bokeh.layouts import column

# Load environment variables from .env file
load_dotenv()

# Database configuration from environment variables
db_host = os.getenv('DB_HOST')
db_name = os.getenv('POSTGRES_DB')
db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')

# Print database connection info (for debugging)
print("Database connection details:")
print(f"Host: {db_host}")
print(f"Database: {db_name}")
print(f"User: {db_user}")

# Create Spark session globally
spark = SparkSession.builder \
    .appName('Data_Engineering_Project') \
    .config("spark.jars",
            "C:\\Users\\admin\\PycharmProjects\\Data_Engineering_Project\\code\\libs\\postgresql-42.7.4.jar") \
    .getOrCreate()

print("Spark session created successfully.")

# Ensure the output directory exists
if not os.path.exists("htdocs"):
    os.makedirs("htdocs")
    print("Created output directory: htdocs")


def getMLResult():
    '''Fetches the machine learning results from the database.'''
    # JDBC URL and connection properties
    jdbc_url = f"jdbc:postgresql://{db_host}/{db_name}"
    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    print("Connecting to the database...")

    # Read data from the Cat_Bill_Amount table
    data = spark.read.jdbc(url=jdbc_url, table='"Cat Bill Amount"', properties=properties)

    if data.count() == 0:
        raise ValueError("No data found in the DataFrame.")

    print(f"Data loaded successfully from Cat_Bill_Amount table. Number of rows: {data.count()}")

    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = data.toPandas()
    print("Converted Spark DataFrame to Pandas DataFrame.")

    # Convert Decimal columns to float
    pandas_df = pandas_df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)

    # Prepare Bokeh data source and table
    source = ColumnDataSource(pandas_df)
    columns = [
        TableColumn(field="medical condition", title="Medical Condition"),
        TableColumn(field="hospital", title="Hospital"),
        TableColumn(field="insurance provider", title="Insurance Provider"),
        TableColumn(field="quarter", title="Quarter"),
        TableColumn(field="billing amount", title="Billing Amount"),
        TableColumn(field="billcat", title="Bill Category"),
    ]

    data_table = DataTable(source=source, columns=columns, width=800, height=800)
    output_file("htdocs/mlresult.html")
    layout = column(data_table)

    save(layout)
    print("Visualization saved to htdocs/mlresult.html")


def getETLResult():
    '''Fetches the ETL results from the database.'''
    # JDBC URL and connection properties
    jdbc_url = f"jdbc:postgresql://{db_host}/{db_name}"
    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    sql_query = """
    (SELECT 
        "Medical Condition", 
        "Hospital", 
        "Insurance Provider", 
        "Quarter", 
        CAST("Discharge Date" AS TEXT) AS discharge_date,
        "Average Bill Amount"  -- Correct column name
    FROM admission) AS subquery
    """

    print("Connecting to the database...")
    data = spark.read.jdbc(url=jdbc_url, table=sql_query, properties=properties)

    if data.count() == 0:
        raise ValueError("No data found in the DataFrame.")

    print(f"Data loaded successfully from admission table. Number of rows: {data.count()}")

    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = data.toPandas()
    print("Converted Spark DataFrame to Pandas DataFrame.")

    # Convert Decimal columns to float if necessary
    for col in pandas_df.select_dtypes(include=['object']).columns:
        if pandas_df[col].apply(lambda x: isinstance(x, decimal.Decimal)).any():
            pandas_df[col] = pandas_df[col].astype(float)

    # Prepare Bokeh data source and table
    source = ColumnDataSource(pandas_df)
    columns = [
        TableColumn(field="Medical Condition", title="Medical Condition"),
        TableColumn(field="Hospital", title="Hospital"),
        TableColumn(field="Insurance Provider", title="Insurance Provider"),
        TableColumn(field="Quarter", title="Quarter"),
        TableColumn(field="discharge_date", title="Discharge Date"),
        TableColumn(field="Average Bill Amount", title="Average Bill Amount"),
    ]

    data_table = DataTable(source=source, columns=columns, width=800, height=800)
    output_file("htdocs/etlresult.html")
    layout = column(data_table)

    save(layout)
    print("Visualization saved to htdocs/etlresult.html")


def getStatus():
    '''Fetches the status information from the database.'''
    # JDBC URL and connection properties
    jdbc_url = f"jdbc:postgresql://{db_host}/{db_name}"
    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    sql_query = """
    (SELECT 
        id, 
        status, 
        message, 
        CAST(timestamp AS TEXT) AS timestamp, 
        CAST(lastloaded AS TEXT) AS lastloaded 
    FROM status) AS subquery
    """

    print("Connecting to the database...")
    data = spark.read.jdbc(url=jdbc_url, table=sql_query, properties=properties)

    if data.count() == 0:
        raise ValueError("No data found in the DataFrame.")

    print(f"Data loaded successfully from status table. Number of rows: {data.count()}")

    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = data.toPandas()
    print("Converted Spark DataFrame to Pandas DataFrame.")

    # Prepare Bokeh data source and table
    source = ColumnDataSource(pandas_df)
    columns = [
        TableColumn(field="id", title="ID"),
        TableColumn(field="status", title="Status"),
        TableColumn(field="message", title="Message"),
        TableColumn(field="timestamp", title="Timestamp"),
        TableColumn(field="lastloaded", title="Last Date Loaded"),
    ]

    data_table = DataTable(source=source, columns=columns, width=800, height=800)
    output_file("htdocs/status.html")
    layout = column(data_table)

    save(layout)
    print("Status visualization saved to htdocs/status.html")


# Execute all functions to generate reports
getMLResult()
getETLResult()
getStatus()
