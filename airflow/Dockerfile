ARG AIRFLOW_IMAGE_TAG=3.2.0-python3.10
FROM apache/airflow:${AIRFLOW_IMAGE_TAG}

# Java configuration
ARG JAVA_VERSION=21
USER root
RUN apt-get update && apt-get install wget gnupg -y && wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | sudo gpg --dearmor -o /usr/share/keyrings/adoptium.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" | sudo tee /etc/apt/sources.list.d/adoptium.list \
    && apt-get update && apt install temurin-${JAVA_VERSION}-jdk -y --no-install-recommends
ENV JAVA_HOME=/
ENV PATH=$JAVA_HOME/bin:$PATH

# PySpark configuration
ARG PYSPARK_VERSION=4.1.1
USER root
RUN apt-get update && apt-get install libkrb5-dev gcc -y --no-install-recommends
USER airflow
COPY docker-requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
