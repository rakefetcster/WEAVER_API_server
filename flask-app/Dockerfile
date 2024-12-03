# Use the official Ubuntu image as the base image
FROM ubuntu:20.04

# Set non-interactive mode for apt-get to avoid prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install Java, Python, and other required packages
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk python3 python3-pip wget curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables for Java and Hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install Hadoop
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xzf hadoop-3.3.6.tar.gz -C /opt/ && \
    rm hadoop-3.3.6.tar.gz

# Set environment variables for Hadoop
ENV HADOOP_HOME=/opt/hadoop-3.3.6
ENV PATH="$PATH:$HADOOP_HOME/bin"

# Download MongoDB Spark Connector (adjust as needed)
RUN wget https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.1.1/mongo-spark-connector_2.12-10.1.1.jar -P /opt/spark/jars/

# Set the working directory for the application
WORKDIR /usr/src/app

# Copy the Python dependencies first to leverage Docker cache
COPY requirements.txt ./

# Install dependencies from requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the Flask application code
COPY app.py ./

# Expose the port your Flask app will run on
EXPOSE 5000

# Set environment variables for Spark (adjust based on your setup)
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3

# Command to run the Flask application
# CMD ["python3", "app.py"] For development or testing 
# For production (e.g., AWS Elastic Beanstalk)
CMD ["gunicorn", "application:app", "-b", "0.0.0.0:5000"] 
