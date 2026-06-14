#!/bin/bash

echo ">> Pulling HDFS image"
docker compose -f hdfs/docker-compose.yml pull

echo ">> Pulling Spark image"
docker compose -f spark/docker-compose.yml pull

echo ">> Pulling MongoDB image"
docker compose -f mongodb/docker-compose.yml pull

echo ">> Pulling and building Airflow image"
docker compose -f airflow/docker-compose.yml pull
docker compose -f airflow/docker-compose.yml build

echo ">> Pulling Metabase image"
docker compose -f metabase/docker-compose.yml pull

echo All images pulled and built!
