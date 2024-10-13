FROM prefecthq/prefect:latest-python3.9

# Install required packages
RUN apt-get update && apt-get install -y \
    default-jdk \
    nginx \
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

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Create application directories
RUN mkdir -p /app/htdocs /app/code

# Set working directory
WORKDIR /app/code

# Copy application code and configuration
COPY code/ /app/code/
COPY code/.env ./
COPY dbconfig.py /app/code/
COPY dbstatus.py /app/code/

# Copy vnisualization files to Nginx's HTML directory
COPY code /visualization/usr/share/nginx/html


# Install Python dependencies
RUN pip install --no-cache-dir \
    psycopg2-binary \
    pandas \
    pyspark \
    python-dotenv \
    prefect \
    prefect-dask \
    bokeh

# Copy Nginx configuration
COPY code/visualization/nginx.conf /etc/nginx/nginx.conf


# Expose Nginx's port
EXPOSE 80

# Start Nginx and run the visualization script
CMD service nginx start && python visualization-process.py
