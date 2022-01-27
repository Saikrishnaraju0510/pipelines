from apache_beam.io.gcp.bigquery import WriteToBigQuery
import argparse
import os
from datetime import date
from google.cloud import bigquery
tab_id='dataflow1.cashpayments'
tab_id2='dataflow1.walletpayments'
tab_id1='dataflow1.cardpayments'
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

parser = argparse.ArgumentParser()
# parser.add_argument('--my-arg')
args, beam_args = parser.parse_known_args()

# Create and set your Pipeline Options.
beam_options = PipelineOptions(
    beam_args,
    runner='DataflowRunner',
    temp_location='gs://us-central1-c1-dea909ea-bucket/temp1',
    service_account_email='demo-py@modular-asset-332406.iam.gserviceaccount.com',
    region='us-central1',
    project='modular-asset-332406',
    stage_location='gs://sai_1999/stage'
    )
#args = beam_options.view_as(MyOptions)
#today= 2548566
client = bigquery.Client()

dataset_id = "modular-asset-332406.dataflow1"

try:
	client.get_dataset(dataset_id)

except:
	dataset = bigquery.Dataset(dataset_id)  #

	dataset.location = "US"
	dataset.description = "dataset"

	dataset_ref = client.create_dataset(dataset)
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
schema_1='Invoice_ID:STRING,Branch:STRING,City:STRING,Customer_type:STRING,Gender:STRING,Product_line:STRING,Unit_price:FLOAT,Quantity:INTEGER,Tax_5:FLOAT,Total:FLOAT,Date:STRING,Time:STRING,Payment:STRING,gross_income:STRING,Rating:STRING'


p=beam.Pipeline(options=beam_options)
data_ingestion=(
    p
    | beam.io.ReadFromText('gs://sai_1999/supermarket_sales.csv',skip_header_lines=1)
    | beam.Map(lambda x:x.split(','))
)
cash_payments=(
    data_ingestion
    | beam.Filter(lambda x:x[12]=="Cash")
    | "FORMAT">>beam.Map(to_json)
    | "write to cash table">> beam.io.WriteToBigQuery(
        tab_id,
        schema=schema_1,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
	      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )
)
wallet_payments=(
    data_ingestion
    | "filter">>beam.Filter(lambda x:x[12]=="Ewallet")
    | "TO_JSON">>beam.Map(to_json)
    | "write to wallet table">> beam.io.WriteToBigQuery(
        tab_id2,
        schema=schema_1,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
	      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )
)
card_payments=(
    data_ingestion
    | "filter2">>beam.Filter(lambda x:x[12]=="Credit card")
    | "WTBQ">>beam.Map(to_json)
    | "write to card table">> beam.io.WriteToBigQuery(
        tab_id1,
        schema=schema_1,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
	      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )
)
p.run()