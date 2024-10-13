from datetime import datetime
import time
import os
import logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import functions as F
from dotenv import load_dotenv
import dbstatus

# Load environment variables from .env file
load_dotenv()

# Accessing PostgreSQL environment variables
db_name = os.getenv('POSTGRES_DB')
db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initializing Spark session
spark = SparkSession.builder \
    .appName('Data_Engineering_Project') \
    .config("spark.jars",
            "C:\\Users\\admin\\PycharmProjects\\Data_Engineering_Project\\code\\libs\\postgresql-42.7.4.jar") \
    .getOrCreate()


def get_last_quarter():
    """Retrieve the last quarter from the status table."""
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
        cursor = connection.cursor()

        cursor.execute("SELECT DISTINCT status FROM status ORDER BY timestamp DESC LIMIT 1;")
        last_quarter = cursor.fetchone()

        return last_quarter[0] if last_quarter else None
    except Exception as e:
        logger.error(f"Error retrieving last quarter: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def create_table():
    """Create the required table for the ML process in the PostgreSQL database."""
    connection = None
    mycursor = None
    try:
        logger.info("Connecting to the database...")
        connection = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
        mycursor = connection.cursor()

        # Check the current and last quarter
        current_quarter = (datetime.now().month - 1) // 3 + 1
        last_quarter = get_last_quarter()

        if current_quarter != last_quarter:
            logger.info("Dropping existing table due to new quarter...")
            mycursor.execute('DROP TABLE IF EXISTS "Cat Bill Amount";')

        # Create the Cat Bill Amount table
        sql_create = """
            CREATE TABLE IF NOT EXISTS "Cat Bill Amount" (
                "Medical Condition" VARCHAR(60),
                "Hospital" VARCHAR(60),
                "Insurance Provider" VARCHAR(60),
                "Quarter" INT,
                "Avg Bill Amount" DECIMAL,
                "Bill Cat" VARCHAR(20)
            );
        """
        logger.info("Executing SQL to create tables...")
        mycursor.execute(sql_create)
        logger.info("Table created successfully.")

        # Clear the table for fresh data
        mycursor.execute("DELETE FROM \"Cat Bill Amount\";")
        logger.info("Cleared Cat_Bill_Amount table.")

        connection.commit()
        logger.info("Changes committed to the database.")

    except Exception as e:
        logger.error(f"Error occurred while creating table: {e}")
        raise
    finally:
        if mycursor:
            mycursor.close()
        if connection:
            connection.close()
            logger.info("Database connection closed.")


def extract():
    """Extract data from the admission table."""
    connection = None
    mycursor = None
    try:
        # Wait for ETL process to finish
        while dbstatus.checkStatus(2):
            logger.info("Waiting for ETL to finish...")
            time.sleep(60)

        logger.info("Logging process status: ML 1/2 - Process started")
        dbstatus.logStatus(3, "ML 1/2 - Process started")

        jdbc_url = f"jdbc:postgresql://{db_host}/{db_name}"
        properties = {
            "user": db_user,
            "password": db_password,
            "driver": "org.postgresql.Driver"
        }

        logger.info("Reading data from the admission table...")
        data = spark.read.jdbc(url=jdbc_url, table="public.admission", properties=properties)

        logger.info("Displaying the first 10 rows of the admission table:")
        data.show(10)

        logger.info("Checking schema of the admission table...")
        data.printSchema()

        # Check for NULL values in the Average Bill Amount column
        null_count = data.filter(data["Average Bill Amount"].isNull()).count()
        logger.info(f"Number of NULL Average Bill Amounts: {null_count}")

        # Create a temporary view for SQL queries
        data.createOrReplaceTempView("admission")

        # Extracting relevant data
        test_sql = """
            SELECT 
                "Medical Condition", 
                "Hospital", 
                "Average Bill Amount"
            FROM 
                admission 
            WHERE 
                "Average Bill Amount" IS NOT NULL;
        """
        logger.info("Executing test SQL query...")
        test_result = spark.sql(test_sql)
        logger.info("Test SQL query executed successfully.")
        test_result.show()

        # Aggregate data using DataFrame API
        aggregated_data = data.groupBy("Medical Condition", "Hospital") \
            .agg(F.round(F.avg("Average Bill Amount"), 2).alias("Avg_Bill_Amount")) \
            .orderBy("Avg_Bill_Amount", ascending=False)

        logger.info("Aggregated data using DataFrame API:")
        aggregated_data.show()

        return aggregated_data

    except Exception as e:
        logger.error(f"Error during data extraction: {e}")
        raise


def create_model(data):
    """Run KMeans clustering on the data."""
    logger.info("Starting KMeans clustering...")

    # Index the Hospital column
    indexer = StringIndexer(inputCol="Hospital", outputCol="Hospital_Index")
    data_indexed = indexer.fit(data).transform(data)

    feature_columns = ["Avg_Bill_Amount", "Hospital_Index"]  # Use indexed column
    logger.info(f"Feature columns used for clustering: {feature_columns}")

    # Assemble feature vector
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    data_with_features = assembler.transform(data_indexed)
    logger.info("Feature vector assembled successfully.")

    # Initialize and fit KMeans model
    kmeans = KMeans(k=3, seed=42)
    model = kmeans.fit(data_with_features)
    logger.info("KMeans model fitted successfully.")

    # Make predictions
    predictions = model.transform(data_with_features)
    logger.info("Predictions made successfully.")

    return predictions.select("Avg_Bill_Amount", "Hospital", "prediction")


def transform(data):
    """Transform the clustered data to include billing categories."""
    logger.info("Transforming clustered data...")

    # Group by prediction and calculate average billing amount
    cluster_metadata = data.groupBy("prediction").agg(F.mean("Avg_Bill_Amount").alias("Avg_Bill_Amount"))
    cluster_metadata = cluster_metadata.orderBy("Avg_Bill_Amount")

    # Create billing categories based on average billing amount
    cluster_metadata = cluster_metadata.withColumn(
        "BillCat",
        F.when(F.col("Avg_Bill_Amount") < 2000.00, "Low")
        .when((F.col("Avg_Bill_Amount") >= 2000.00) & (F.col("Avg_Bill_Amount") < 4000.00), "Moderate")
        .when((F.col("Avg_Bill_Amount") >= 4000.00) & (F.col("Avg_Bill_Amount") < 6000.00), "High")
        .otherwise("Substantial")
    )

    logger.info("Joining cluster metadata with original data...")
    data = data.join(cluster_metadata.select("prediction", "BillCat"), on="prediction", how="left")
    logger.info("Data joined successfully. Dropping prediction column...")

    # Rename columns to match database schema
    data = data.withColumnRenamed("Avg_Bill_Amount", "Avg Bill Amount") \
        .withColumnRenamed("BillCat", "Bill Cat")

    return data.drop("prediction")


def load(data):
    """Load the processed data into the PostgreSQL database."""
    logger.info("Preparing to load data into the database...")

    db_url = f"jdbc:postgresql://{db_host}/{db_name}"
    db_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    logger.info("Writing data to the Cat_Bill_Amount table...")

    try:
        # Write the DataFrame to the PostgreSQL table
        data.write.jdbc(url=db_url, table="\"Cat Bill Amount\"", mode="append", properties=db_properties)
        logger.info("Data loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        raise

    logger.info("Process completed successfully.")
    return "--- Process successfully completed! ---"


def main():
    """Main entry point of the ETL process."""
    create_table()
    extracted_data = extract()

    if extracted_data is not None:
        logger.info("Extracted Data:")
        extracted_data.show()

        # Create the model using the extracted data
        model = create_model(extracted_data)

        # Log the model's predictions
        logger.info("Model predictions:")
        model.show()

        # Transform the predictions to include billing categories
        transformed_data = transform(model)

        # Log the transformed data
        logger.info("Transformed Data with Billing Categories:")
        transformed_data.show()

        # Load the transformed data into the database
        load(transformed_data)

        # Update the status table with the current quarter
        current_quarter = (datetime.now().month - 1) // 3 + 1
        dbstatus.logStatus(current_quarter, "ML process completed for the quarter.")


if __name__ == "__main__":
    main()
