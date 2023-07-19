FROM python:3.9

# Install Java (required for Apache Spark)
RUN apt-get update && apt-get install -y default-jdk

# Set the Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Install Apache Spark
RUN curl -O https://downloads.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz \
    && tar xvf spark-3.4.1-bin-hadoop3.tgz \
    && mv spark-3.4.1-bin-hadoop3 /opt/spark \
    && rm spark-3.4.1-bin-hadoop3.tgz

# Install PySpark and JDBC drivers
RUN pip install pyspark pyodbc

# Copy and install the MSSQL JDBC driver
COPY mssql-jdbc-12.2.0.jre8.jar $SPARK_HOME/jars/

# Set the working directory
WORKDIR /barbora_DE_task

# Copy your application files into the container
COPY . /barbora_DE_task

# Install virtualenv
RUN python -m pip install --upgrade pip virtualenv

# Create and activate the virtual environment
RUN python -m virtualenv venv
ENV PATH="/barbora_DE_task/venv/Scripts:$PATH"

# Install dependencies
RUN pip install -r requirements.txt
RUN pip install pydantic[email]


ENV DB_USERNAME=${DB_USERNAME}
ENV DB_PASSWORD=${DB_PASSWORD}


# Start your application
CMD ["pytest"]
