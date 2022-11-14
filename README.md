# Dataflow BigQuery Migration
A simple Apache Beam pipeline to migrate data from a BigQuery table to a new schema specified by a configuration JSON to be run on Google Cloud Dataflow.

## Setting up Configuration Files
Configurations are based on BigQuery schema structure and datatypes in JSON. [See official BigQuery documentation](https://cloud.google.com/bigquery/docs/schemas#specifying_a_json_schema_file)
