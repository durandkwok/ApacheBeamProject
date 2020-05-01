# Process
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import window

# Replace with your service account path
#service_account_path = '/Users/dkwok/Downloads/BeamTest/gcp-se-fe24e05febf1.json'
service_account_path = 'gcp-se-fe24e05febf1.json'

#/Users/dkwok/Downloads/BeamTest

print("Service account file : ", service_account_path)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

# Replace with your input subscription id
input_subscription = 'projects/gcp-se/subscriptions/Subscribe1test'

# Replace with your output subscription id
output_topic = 'projects/gcp-se/topics/Topic2test'

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)
output_file = 'outputs/part'

pubsub_data = (
              p
               | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
               | 'Write to pub sub' >> beam.io.WriteToPubSub(output_topic)
              )
result = p.run()
result.wait_until_finish()

