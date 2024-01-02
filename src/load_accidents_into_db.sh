#!/bin/bash

# Define parameters
GEOJSON_FILE=$1
DB_NAME=$2
DB_USER=$3
DB_PASSWORD=$4
DB_HOST=$5
DB_PORT=$6
TARGET_TABLE=$7

# Run ogr2ogr command
ogr2ogr -f "PostgreSQL" PG:"dbname='$DB_NAME' host='$DB_HOST' port='$DB_PORT' user='$DB_USER' password='$DB_PASSWORD'" "$GEOJSON_FILE" -nln $TARGET_TABLE -append

echo "GeoJSON data has been imported into $TARGET_TABLE"
