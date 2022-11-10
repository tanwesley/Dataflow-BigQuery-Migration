import argparse
import apache_beam as beam 
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions 
import json
import gcsfs


BUCKET_NAME = 'bq-data-migration-store'
PROJECT_NAME = 'cloudbuild-test-367215'
OUTPUT_DATASET = 'test_dataset'
OUTPUT_TABLE = 'new_test_table'

GCS_FILE_SYSTEM = gcsfs.GCSFileSystem(project=PROJECT_NAME)
# GCS_CONFIG_PATH = 'gs://bq-data-migration-store/bq-migrate-config.json'
# GCS_CONFIG_PATH = 'gs://bq-data-migration-store/migrate_config.json'
# GCS_CONFIG_PATH = 'gs://bq-data-migration-store/migrate_config_2.json'
GCS_CONFIG_PATH = 'gs://bq-data-migration-store/migrate_config_v2.json'

update_config = json.load(GCS_FILE_SYSTEM.open(GCS_CONFIG_PATH))

GCS_NEW_SCHEMA_PATH = 'gs://bq-data-migration-store/new_bqschema.json'
# new_schema = json.load(GCS_FILE_SYSTEM.open(GCS_NEW_SCHEMA_PATH))

def data_conversion(data, old_type, new_type):
    if new_type == 'STRING': 
        converted = str(data)
    elif new_type == 'BOOLEAN':
        if data == 'true' | data == 'True':
            converted = True
        elif data == 'false' | data == 'False':
            converted = False
        else: 
            converted = None
    elif new_type == 'INTEGER':
        try:
            converted = int(data)
        except: 
            converted = None 
    elif new_type == 'FLOAT': 
        try: 
            converted = float(data)
        except:
            converted = None 

    return converted

def old_to_new_schema(data: dict, config: list = update_config): 
    import logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    for c in config: 
        name = c.get('name')

        if c.get('mode') == 'REPEATED':             
            logger.info(f"DATA: {data}")
            logger.info(f"NESTED FIELDS: {data.get(name)}")
            nested_fields = data.get(name) 

            for f in nested_fields: 
                data.update({ name: old_to_new_schema(f, c.get('fields')) })
                logger.info(f"UPDATED DATA: {data}")

        else:
            mutation_type = c.get('mutation_type') 

            if mutation_type == 'add': 
                value = c.get('default_value')
                data.update({ name: value })
            elif mutation_type == 'modify':
                value = data_conversion(data.get(name), data.get('type'), c.get('type'))
                data.update({ name: value })
            elif mutation_type == 'delete':
                data.pop(name)

    logger.info(f"FINAL UPDATED DATA: {data}\n")
    return [data]


def run(argv=None):

    parser = argparse.ArgumentParser()

    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        default=f'{PROJECT_NAME}:{OUTPUT_DATASET}.{OUTPUT_TABLE}',
                        help='Output file to write results to.')


    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=pipeline_options)

    main = (p
        | 'Read old table' >> (beam.io.ReadFromBigQuery(gcs_location='gs://bq-data-migration-store/test-table',
                                                        table='cloudbuild-test-367215:test_dataset.test-table'))
        | 'Convert to new schema' >> beam.ParDo(old_to_new_schema) 
        | 'Write to BigQuery' >> (beam.io.WriteToBigQuery(table=known_args.output, 
                                                          custom_gcs_temp_location='gs://bq-data-migration-store/temp')
        )
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run()

# python data_migration_test.py --project=cloudbuild-test-367215 \
# --output cloudbuild-test-367215:test_dataset.new_test_table \
# --temp_location=gs://bq-data-migration-store/temp/ \
# --project=cloudbuild-test-367215 \
# --region=us-central1
