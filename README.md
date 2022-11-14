# Dataflow BigQuery Migration
A simple Apache Beam pipeline to migrate data from a BigQuery table to a new schema specified by a configuration JSON to be run on Google Cloud Dataflow.

## Setting up Configuration Files
Configurations are based on BigQuery schema structure and datatypes using JSON. [See official BigQuery documentation](https://cloud.google.com/bigquery/docs/schemas#specifying_a_json_schema_file)

To write an update configuration JSON, write a BigQuery JSON schema containing only the JSON objects for columns that will be changed (added/deleted/modified), with the addition of additional fields: 
- "mutation_type": The type of change to be applied to match the new schema.
  - Options: "add", "modify", "delete".
    - "add" is for new columns.
    - "modify" is for modifying the datatype of an existing column.
    - "delete" is for removing columns you do not wish to migrate.
- "default_value": The value to be set for the column when adding a column, or modifying a column if a conversion between types is invalid.

### Example:
#### Source BigQuery table schema:
```
[
  {
    "name": "first_name",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "last_name",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "phone_number",
    "type": "INTEGER",
    "mode": "NULLABLE"
  }
]
```

#### Target BigQuery table schema:
```
[
  {
    "name": "last_name",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "phone_number",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "address",
    "type": "STRING",
    "mode": "NULLABLE"
  }
]
```

#### Migration configuration:
- Removes "first_name" field.
- Changes "phone_number field from INTEGER to STRING
- Adds "address" field.
```
[
  {
    "name": "first_name",
    "type": "STRING",
    "mode": "REQUIRED",
    "mutation_type": "delete"
  },
  {
    "name": "phone_number",
    "type": "STRING",
    "mode": "NULLABLE",
    "mutation_type": "modify",
    "default_value": null
  },
  {
    "name": "address",
    "type": "STRING",
    "mode": "NULLABLE",
    "mutation_type": "add",
    "default_value": null
  }
]
```
Create a BigQuery table with the new schema JSON specification and store the update configuration JSON in a GCS bucket.
