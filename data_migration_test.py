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

class Printer(beam.DoFn):
    def process(self, element):
        print('\ndata: ')
        print(element)
        print('type: ')
        print(type(element))
        # print('Length: ', len(element))


# class OldToNewSchema(beam.DoFn): 
#     def process(self, data, config):
#         # data.update({'hello': 'world'})
#         for key in config:
#             data.update([{key: config[key]}])
#         # Modify fields to match new schema 
#         return [data]


# TO DO: Add conversion for nested columns.
# TO DO: job works on DirectRunner but fails on DataflowRunner. old_to_new_schema function appears to not be defined when
#        running pipeline.
def old_to_new_schema(data: dict, config: list = update_config): 
    import logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    for d in config: 
        name = d.get('name')

        if d.get('mode') == 'REPEATED':             
            logger.info(f"DATA: {data}")
            logger.info(f"NESTED FIELDS: {data.get(name)}")
            nested_fields = data.get(name) 

            for f in nested_fields: 
                data.update({ name: old_to_new_schema(f, d.get('fields')) })
                logger.info(f"UPDATED DATA: {data}")

        else:
            mutation_type = d.get('mutation_type') 

            if mutation_type == 'add': 
                value = d.get('default_value')
                data.update({ name: value })
            elif mutation_type == 'modify':
                value = d.get('set_value')
                data.update({ name: value })
            elif mutation_type == 'delete':
                data.pop(name)

    logger.info(f"FINAL UPDATED DATA: {data}\n")
    return [data]

    # for d in config:
    #     change_type = d.get('Type')
    #     field_name = d.get('Field')
    #     default_val = d.get('Default')

    #     # TO DO: Make this more scalable and clean
    #     if '.' in field_name:
    #         super_field, sub_field = field_name.split('.')
    #         logger.info(f' super_field={super_field}, sub_field={sub_field}')

    #         nested_obj = data.get(super_field)[0]
    #         logger.info(f' Nested Object: {nested_obj}')

    #         if change_type == 'new':
    #             nested_obj.update({ sub_field: default_val })
    #             logger.info(f' Updated nested obj: {nested_obj}')
    #             data.update({ super_field: [nested_obj] })
    #         elif change_type == 'modify': 
    #             nested_obj.update({ sub_field: default_val })
    #             logger.info(f' Updated nested obj: {nested_obj}')
    #             data.update({ super_field: [nested_obj] })
    #         elif change_type == 'delete':
    #             nested_obj.pop(sub_field)
    #             logger.info(f' Updated nested obj: {nested_obj}')
    #             data.update({ super_field: [nested_obj] })
                
    #     else: 
    #         if change_type == 'new':
    #             if data.get(field_name) != None: 
    #                 raise Exception('Field already exists in old table!')
    #             data.update({ field_name: default_val })
    #         elif change_type == 'modify': 
    #             if data.get(field_name) == None: 
    #                 raise Exception('Field does not exist in the old table!')
    #         elif change_type == 'delete':
    #             if data.get(field_name) == None: 
    #                 raise Exception('Field does not exist in the old table!')
    #             data.pop(field_name)

    #     logger.info(f'\nModification: {change_type}, Field: {field_name}\n\n')

    # logger.info(f'\nConverted Data: {data} \nType: {type(data)}\n\n')
    # return [data]


        
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
