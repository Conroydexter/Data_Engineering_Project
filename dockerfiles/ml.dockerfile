FROM prefecthq/prefect:latest-python3.9

# Required packages
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Spark
ENV SPARK_VERSION=3.3.1
ENV HADOOP_VERSION=3.2

RUN curl -sL https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -o spark.tgz && \
    tar -xzf spark.tgz -C /opt/ && \
    ln -s /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark && \
    rm spark.tgz  # Clean up the tarball after extraction

# Environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Create application directories
RUN mkdir -p /app/ml /app/data

# Set working directory
WORKDIR /app/ml

# Copy your application code
COPY code/ /app/ml/
COPY code/.env ./
COPY dbconfig.py /app/
COPY dbstatus.py /app/

# Install Python dependencies
RUN pip install --no-cache-dir \
    psycopg2-binary \
    pandas \
    pyspark \
    python-dotenv \
    prefect

# Command to run your application
CMD ["python", "ml-process.py"]
