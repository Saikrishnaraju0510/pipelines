from apache_beam.transforms.window import FixedWindows
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
import os
import time
from apache_beam.options.pipeline_options import SetupOptions
from datetime import date
from google.cloud import bigquery
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount, Repeatedly

tab_id='airflow3.payments'
import apache_beam as beam

parser = argparse.ArgumentParser()
# parser.add_argument('--my-arg')
args, beam_args = parser.parse_known_args()

# Create and set your Pipeline Options.
beam_options = PipelineOptions(
    beam_args,
    runner='DataflowRunner',
    temp_location='gs://raju0510/',
    service_account_email=' sai-314@brave-set-369316.iam.gserviceaccount.com',
    region='europe-north1',
    project='brave-set-369316',
    stage_location='gs://raju0510/stage',
    #save_main_session=True
    )
beam_options.view_as(StandardOptions).streaming=True
#beam_options.view_as(SetupOptions).save_main_session = True
client = bigquery.Client()

dataset_id = "brave-set-369316.airflow3"

try:
	client.get_dataset(dataset_id)

except:
	dataset = bigquery.Dataset(dataset_id)  #

	dataset.location = "EU"
	dataset.description = "dataset"

	dataset_ref = client.create_dataset(dataset)
 
def decoding(e):
  e=e.decode('utf-8')
  return e[:-2]
 
input_subscription='projects/brave-set-369316/subscriptions/beam2-sub'
def to_json(fields):
    json_str = {"Invoice_ID":fields[0],
                 "Branch": fields[1],
                 "City": fields[2],
                 "Customer_type": fields[3],
                 "Gender": fields[4],
                 "Product_line": fields[5],
                 "Unit_price": fields[6],
                 "Quantity": fields[7],
                 "Tax_5": fields[8],
                 "Total": fields[9],
                 "Date": '256485',
                 "Time": fields[11],
                 "Payment": fields[12],
                 "gross_income": fields[15],
                 "Rating":fields[16]
                 }
    return json_str
schema_1='Invoice_ID:STRING,Branch:STRING,City:STRING,Customer_type:STRING,Gender:STRING,Product_line:STRING,Unit_price:STRING,Quantity:STRING,Tax_5:STRING,Total:STRING,Date:STRING,Time:STRING,Payment:STRING,gross_income:STRING,Rating:STRING'
with beam.Pipeline(options=beam_options) as pipeline:
  data_ingestion=(
    pipeline
    | beam.io.ReadFromPubSub(subscription=input_subscription,timestamp_attribute=None)
    | beam.Map(decoding)
    | beam.Map(lambda x:x.split(','))
    | beam.Map(to_json)
    | beam.WindowInto(beam.window.FixedWindows(5),trigger=AfterCount(3),accumulation_mode=AccumulationMode.DISCARDING)
    | beam.io.WriteToBigQuery(
                             tab_id,
                             schema=schema_1,
                             method=                                                                                
                     beam.io.WriteToBigQuery.Method.FILE_LOADS,
                             triggering_frequency=1, 
                             write_disposition=
                     beam.io.BigQueryDisposition.WRITE_APPEND,
                             create_disposition=
                     beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        
    )
)
