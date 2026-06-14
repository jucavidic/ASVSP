#!/bin/bash

echo ">> Creating f1-analysis network"
docker network create f1-analysis

echo ">> Starting up HDFS"
docker compose -f hdfs/docker-compose.yml up -d

echo ">> Starting up Spark"
docker compose -f spark/docker-compose.yml up -d

echo ">> Starting up MongoDB"
docker compose -f mongodb/docker-compose.yml up -d

echo ">> Starting up Airflow"
docker compose -f airflow/docker-compose.yml up -d

echo ">> Starting up Metabase"
docker compose -f metabase/docker-compose.yml up -d

echo Cluster is up!
