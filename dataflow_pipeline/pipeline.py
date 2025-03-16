import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
# from datetime import datetime


# A custom DoFn to parse CSV records.
class ParseCSV(beam.DoFn):
    def process(self, element):
        # Split the CSV line by commas into individual fields.
        fields = element.split(",")
        # Return a dictionary with the appropriate types for each column.
        return [{
            'vendor_id': int(fields[0]),
            'tpep_pickup_datetime': fields[1],
            'tpep_dropoff_datetime': fields[2],
            'passenger_count': int(fields[3]),
            'trip_distance': float(fields[4]),
            'rate_code_id': int(fields[5]),
            'store_and_fwd_flag': fields[6],
            'pu_location_id': int(fields[7]),
            'do_location_id': int(fields[8]),
            'payment_type': int(fields[9]),
            'fare_amount': float(fields[10]),
            'extra': float(fields[11]),
            'mta_tax': float(fields[12]),
            'tip_amount': float(fields[13]),
            'tolls_amount': float(fields[14]),
            'improvement_surcharge': float(fields[15]),
            'total_amount': float(fields[16]),
            'congestion_surcharge': float(fields[17]),
            'airport_fee': float(fields[18])
        }]


# A custom DoFn to transform and enrich data.
class TransformData(beam.DoFn):
    def process(self, element):
        from datetime import datetime
        import uuid

        # Parse the pickup and dropoff datetime strings into datetime objects.
        pickup_datetime = datetime.strptime(
            element['tpep_pickup_datetime'], "%Y-%m-%d %H:%M:%S")
        dropoff_datetime = datetime.strptime(
            element['tpep_dropoff_datetime'], "%Y-%m-%d %H:%M:%S")

        # Define a baseline date for creating a date_id key.
        baseline_date = datetime(2024, 1, 1)

        # Create a calendar dimension record to capture date-related attributes.
        calendar_data = {
            # Calculate a date identifier as the number of days since the baseline.
            'date_id': (pickup_datetime.date() - baseline_date.date()).days + 1,
            'date': pickup_datetime.strftime("%Y-%m-%d"),
            'year': pickup_datetime.year,
            'quarter': (pickup_datetime.month - 1) // 3 + 1,
            'month': pickup_datetime.month,
            'day': pickup_datetime.day,
            'day_of_week': pickup_datetime.strftime("%A")
        }

        # Create a time dimension record for pickup time attributes.
        pickup_time_data = {
            # Create a time key based on the seconds passed since midnight.
            'time_id': int((pickup_datetime.hour * 3600 + pickup_datetime.minute * 60 + pickup_datetime.second) + 1),
            'hour': pickup_datetime.hour,
            'minute': pickup_datetime.minute,
            'second': pickup_datetime.second,
            'period': 'AM' if pickup_datetime.hour < 12 else 'PM'
        }

        # Create a fact table record that includes detailed trip information.
        fact_data = {
            # Generate a unique trip identifier.
            'trip_id': str(uuid.uuid4()),
            'vendor_id': element['vendor_id'],
            'pickup_datetime': pickup_datetime,
            'dropoff_datetime': dropoff_datetime,
            # Use similar time key calculation for both pickup and dropoff times.
            'pickup_time_id': int((pickup_datetime.hour * 3600 + pickup_datetime.minute * 60 + pickup_datetime.second)+1),
            'dropoff_time_id': int((dropoff_datetime.hour * 3600 + dropoff_datetime.minute * 60 + dropoff_datetime.second)+1),
            # Calculate date ids based on the difference from the baseline date.
            'pickup_date_id': (pickup_datetime.date()-baseline_date.date()).days+1,
            'dropoff_date_id': (dropoff_datetime.date()-baseline_date.date()).days+1,
            'passenger_count': element['passenger_count'],
            'trip_distance': element['trip_distance'],
            'store_and_fwd_flag': element['store_and_fwd_flag'],
            'pickup_location_id': element['pu_location_id'],
            'dropoff_location_id': element['do_location_id'],
            'fare_amount': element['fare_amount'],
            'extra': element['extra'],
            'mta_tax': element['mta_tax'],
            'tip_amount': element['tip_amount'],
            'tolls_amount': element['tolls_amount'],
            'improvement_surcharge': element['improvement_surcharge'],
            'total_amount': element['total_amount'],
            'congestion_surcharge': element['congestion_surcharge'],
            'airport_fee': element['airport_fee'],
            'payment_type': element['payment_type'],
            # Format the pickup date for reference.
            'trip_date': pickup_datetime.strftime("%Y-%m-%d")
        }

        # Emit a tuple with a key indicating the target table and the transformed record.
        yield ('fact_table', fact_data)
        yield ('calendar_dim', calendar_data)
        yield ('time_dim', pickup_time_data)


# A partition function to route records to different output tables.
def partition_fn(element, num_partitions):
    # Define a mapping from table key to partition index.
    mapping = {
        'fact_table': 0,
        'calendar_dim': 1,
        'time_dim': 2
    }
    key, _ = element
    # Return the partition index corresponding to the key.
    # If the key does not match, default to the last partition.
    return mapping.get(key, num_partitions - 1)


import json
from apache_beam.io.filesystems import FileSystems
# Function to load a JSON schema from GCS.
def load_schema_from_gcs(gcs_path):
    with FileSystems.open(gcs_path) as f:
        schema = json.load(f)
    # If the schema is a list, wrap it in {"fields": ...}
        if isinstance(schema, list):
            schema = {"fields": schema}
        return schema



def run():
    # Load the schemas from GCS.
    trip_records_schema = load_schema_from_gcs('gs://dataflow_pipeline_nyc_taxi/table_schemas/trip_records.json')
    calendar_dim_schema = load_schema_from_gcs('gs://dataflow_pipeline_nyc_taxi/table_schemas/calendar_dim.json')
    time_dim_schema = load_schema_from_gcs('gs://dataflow_pipeline_nyc_taxi/table_schemas/time_dim.json')

    # Define a custom PipelineOption to pass additional command-line parameters.
    class CustomOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            # Add a custom argument 'bus_date' to specify the date for input file selection.
            parser.add_argument(
                '--bus_date',
                help='Date in YYYY-MM-DD format to build the input file name',
                required=True
            )

    # Retrieve standard and custom pipeline options.
    options = PipelineOptions()
    custom_options = options.view_as(CustomOptions)
    bus_date = custom_options.bus_date  # e.g., '2024-01-01'
    file_path = f'gs://nyc_taxi_raw_data/tripdata_{bus_date}.csv'

    # Define the Beam pipeline using the provided options.
    with beam.Pipeline(options=options) as p:
        # Read CSV file from GCS, skipping the header line.
        data = (
            p
            | 'Read Input' >> beam.io.ReadFromText(file_path, skip_header_lines=1)
            | 'Parse CSV' >> beam.ParDo(ParseCSV())  # Parse each CSV line into a dictionary.
            | 'Transform Data' >> beam.ParDo(TransformData())  # Transform and enrich each record.
        )

        # Partition the transformed data into multiple collections based on target table.
        partitions = data | 'Partition by Table' >> beam.Partition(
            partition_fn, 3)

        # Extract fact table records (first partition) and write to BigQuery.
        partitions[0] \
            | 'Extract fact_data' >> beam.Map(lambda x: x[1]) \
            | 'Write fact_table to BigQuery' >> beam.io.WriteToBigQuery(
                table='nyc-taxi-batch-dataflow:trip_data.trip_records',
                schema=trip_records_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )

        # Extract calendar dimension records (second partition) and write to BigQuery.
        partitions[1] \
            | 'Extract calendar_data' >> beam.Map(lambda x: x[1]) \
            | 'Write calendar_dim to BigQuery' >> beam.io.WriteToBigQuery(
                table='nyc-taxi-batch-dataflow:trip_data.calendar_dim_staging',
                schema=calendar_dim_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )

        # Extract time dimension records (third partition) and write to BigQuery.
        partitions[2] \
            | 'Extract time_data' >> beam.Map(lambda x: x[1]) \
            | 'Write time_dim to BigQuery' >> beam.io.WriteToBigQuery(
                table='nyc-taxi-batch-dataflow:trip_data.time_dim_staging',
                schema=time_dim_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )


# Execute the pipeline when the script is run directly.
if __name__ == '__main__':
    run()
