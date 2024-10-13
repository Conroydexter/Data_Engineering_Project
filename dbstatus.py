import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def logStatus(status: int, message: str, lastLoaded: str = ""):
    '''Log status information about the current process.'''
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        with psycopg2.connect(
            host=os.getenv('DB_HOST'),
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        ) as connection:
            with connection.cursor() as mycursor:
                if lastLoaded:
                    sql = "INSERT INTO status (status, message, timestamp, lastloaded) VALUES (%s, %s, %s, %s);"
                    mycursor.execute(sql, (status, message, now, lastLoaded))
                else:
                    sql = "INSERT INTO status (status, message, timestamp) VALUES (%s, %s, %s);"
                    mycursor.execute(sql, (status, message, now))

                connection.commit()
                logger.info("Status logged successfully.")

    except Exception as e:
        logger.error(f"Error during logging status: {e}")

def checkStatus(status_code: int) -> bool:
    """Check the status from the database."""
    try:
        with psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('DB_HOST')
        ) as connection:
            with connection.cursor() as cursor:
                query = "SELECT status FROM status WHERE id = %s;"
                cursor.execute(query, (status_code,))
                result = cursor.fetchone()

                if result is None:
                    logger.warning(f"No status found for status code {status_code}.")
                    return False

                return result[0] == 1  # Adjust as needed for your logic

    except Exception as e:
        logger.error(f"Error checking status: {e}")
        return False
