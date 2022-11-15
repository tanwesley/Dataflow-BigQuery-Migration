import argparse
import apache_beam as beam 
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions 
import json
import gcsfs

class OldToNewSchema(beam.DoFn):
    def __init__(self, config):
        self.config = config

    def process(self, data, config=None):
        import logging
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        if config == None: 
            config = self.config 
        
        for c in config: 
            name = c.get('name')

            if c.get('mode') == 'REPEATED':             
                logger.info(f"DATA: {data}")
                logger.info(f"NESTED FIELDS: {data.get(name)}")
                nested_fields = data.get(name) 

                for f in nested_fields: 
                    data.update({ name: self.process(data=f, config=c.get('fields')) })
                    logger.info(f"UPDATED DATA: {data}")

            else:
                mutation_type = c.get('mutation_type') 

                if mutation_type == 'add': 
                    value = c.get('default_value')
                    data.update({ name: value })
                elif mutation_type == 'modify':
                    value = self.data_conversion(data.get(name), data.get('type'), c.get('type'))
                    data.update({ name: value })
                elif mutation_type == 'delete':
                    data.pop(name)

        logger.info(f"FINAL UPDATED DATA: {data}\n")
        return [data]

    def data_conversion(self, data, old_type, new_type):
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


def run(argv=None):

    parser = argparse.ArgumentParser()

    
    # Arguments to execute Python script with.
    # Required arguments: 
    # --input, --migrate_config, --output, --project, --temp_location
    parser.add_argument('--input',
                        dest='input',
                        required=True,
                        help='Input data from BigQuery table to migrate. \nFormat: your_project_name:your_dataset.output_table')
    parser.add_argument('--migrate_config',
                        dest='migrate_config',
                        required=True,
                        help='JSON configuration for migrating data from old table to the new schema.')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='BigQuery table to write results to. \nFormat: your_project_name:your_dataset.output_table')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options_dict = pipeline_options.get_all_options()

    # Get values from pipeline_options arguments
    project_name = pipeline_options_dict.get('project')
    temp_location = pipeline_options_dict.get('temp_location')
    if temp_location == None: 
        raise Exception('Missing required flag: --temp_location. --temp_location should be a path to a GCS location to write data to BigQuery.')

    # API to access Google Cloud Storage file system. 
    # Read JSON configuration file from GCS and load as Dictionary
    fs = gcsfs.GCSFileSystem(project=project_name)
    update_config = json.load(fs.open(known_args.migrate_config))

    p = beam.Pipeline(options=pipeline_options)

    (p
        | 'Read old table' >> (beam.io.ReadFromBigQuery(table=known_args.input, gcs_location=temp_location))
        | 'Convert to new schema' >> beam.ParDo(OldToNewSchema(update_config)) 
        | 'Write to BigQuery' >> (beam.io.WriteToBigQuery(table=known_args.output, 
                                                          custom_gcs_temp_location=temp_location))
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run()

