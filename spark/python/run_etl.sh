#!/usr/bin/env bash

$SPARK_HOME/bin/spark-submit \
  --py-files packages.zip \
  --files configs/museums_etl_config.json \
  jobs/museums_etl.py

