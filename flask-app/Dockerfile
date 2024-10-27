# Use the official Ubuntu image as the base image
FROM ubuntu:20.04

# Set non-interactive mode for apt-get
ENV DEBIAN_FRONTEND=noninteractive

# Install Java and Python
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk python3 python3-pip wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install Hadoop (use actual version number)
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xzf hadoop-3.3.6.tar.gz -C /opt/ && \
    rm hadoop-3.3.6.tar.gz

# Set environment variables for Hadoop
ENV HADOOP_HOME=/opt/hadoop-3.3.6
ENV PATH="$PATH:$HADOOP_HOME/bin"

# Download MongoDB Spark Connector (version 10.1.1)
RUN wget https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.1.1/mongo-spark-connector_2.12-10.1.1.jar -P /opt/spark/jars/

# Set the working directory
WORKDIR /usr/src/app

# Copy requirements and install dependencies
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the Flask app files
COPY app.py ./

# Expose the port the app runs on
EXPOSE 5000

# Command to run the Flask application
CMD ["python3", "app.py"]
