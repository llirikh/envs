#!/bin/bash

# Файл для загрузки
LOCAL_FILE="data/test.csv"
HDFS_DIR="/data"
HDFS_FILE="${HDFS_DIR}/test.csv"
NAMENODE_CONTAINER="hdfs-namenode-1"

echo "Копируем файл в контейнер namenode..."
docker cp "$LOCAL_FILE" $NAMENODE_CONTAINER:/tmp/test.csv
if [ $? -ne 0 ]; then
    echo "Ошибка при копировании файла!"
    exit 1
fi

echo "Создаём каталог в HDFS: $HDFS_DIR"
docker exec -it $NAMENODE_CONTAINER hdfs dfs -mkdir -p "$HDFS_DIR"
if [ $? -ne 0 ]; then
    echo "Ошибка при создании каталога в HDFS!"
    exit 1
fi

echo "Даем права на запись всем пользователям для $HDFS_DIR"
docker exec -it $NAMENODE_CONTAINER hdfs dfs -chmod 777 "$HDFS_DIR"
if [ $? -ne 0 ]; then
    echo "Ошибка при изменении прав на каталог HDFS!"
    exit 1
fi

echo "Заливаем файл в HDFS: $HDFS_FILE"
docker exec -it $NAMENODE_CONTAINER hdfs dfs -put -f /tmp/test.csv "$HDFS_FILE"
if [ $? -ne 0 ]; then
    echo "Ошибка при загрузке файла в HDFS!"
    exit 1
fi

echo "Содержимое каталога $HDFS_DIR в HDFS:"
docker exec -it $NAMENODE_CONTAINER hdfs dfs -ls "$HDFS_DIR"
