#!/bin/bash

echo "1. Copy breweries.csv to the namenode:"
docker cp breweries.csv namenode:breweries.csv
if [ $? -ne 0 ]; then
    echo "ERROR!"
    exit 1
fi

echo "2. Create a HDFS directory /data//openbeer/breweries."
docker exec -it namenode bash hdfs dfs -mkdir -p /data/openbeer/breweries
if [ $? -ne 0 ]; then
    echo "ERROR!"
    exit 1
fi

echo "3. Copy breweries.csv to HDFS:"
docker exec -it namenode bash hdfs dfs -put breweries.csv /data/openbeer/breweries/breweries.csv
if [ $? -ne 0 ]; then
    echo "ERROR!"
    exit 1
fi