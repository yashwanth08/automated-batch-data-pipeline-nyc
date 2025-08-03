FROM apache/airflow:2.7.1

USER root

# Install Java (Spark requires it)
RUN apt-get update && apt-get install -y openjdk-11-jdk curl

# Download and install Spark
#RUN curl -O https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz \
    #&& tar -xvzf spark-3.3.0-bin-hadoop3.tgz \
    #&& mv spark-3.3.0-bin-hadoop3 /opt/spark \
    #&& rm spark-3.3.0-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="$PATH:/opt/spark/bin"

# Switch to airflow user, then install pip packages
USER airflow

#RUN pip install apache-airflow-providers-apache-spark==4.1.5


# Install BigQuery client
RUN pip install google-cloud-bigquery==3.12.0