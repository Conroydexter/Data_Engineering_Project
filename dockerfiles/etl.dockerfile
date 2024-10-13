FROM prefecthq/prefect:latest-python3.9

# Install dependencies
RUN apt-get update && apt-get install -y openjdk-11-jdk curl

# Install Spark
ENV SPARK_VERSION=3.3.1
ENV HADOOP_VERSION=3.2

RUN curl -sL https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -o spark.tgz && \
    tar -xzf spark.tgz -C /opt/ && \
    ln -s /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark && \
    rm spark.tgz


# Environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Python dependencies
RUN pip install psycopg2-binary pandas python-dotenv

# Create directories
RUN mkdir -p /app/etl /app/data

# Set working directory
WORKDIR /app

# Required files
COPY code/etl etl/
COPY code/.env ./
COPY dbconfig.py /app/
COPY dbstatus.py /app/
COPY data/ /app/data/


# Command to run the application
CMD ["python", "etl/etl_process.py"]


