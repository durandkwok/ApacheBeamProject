# Process
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount


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

def encode_byte_string(element):
   print element
   element = str(element)
   return element.encode('utf-8')

def calculateProfit(elements):
  buy_rate = elements[5]
  sell_price = elements[6]
  products_count = int(elements[4])
  profit = (int(sell_price) - int(buy_rate)) * products_count
  elements.append(str(profit))
  return elements


pubsub_data = (
                p 
                | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= input_subscription)
                | 'Remove extra chars' >> beam.Map(lambda data: (data.rstrip().lstrip()))
                | 'Split Row' >> beam.Map(lambda row : row.split(','))
                | 'Filter By Country' >> beam.Filter(lambda elements : (elements[1] == "Maine" or elements[1] == "Texas"))
                | 'Create Profit Column' >> beam.Map(calculateProfit)
                #| 'Apply custom timestamp' >> beam.Map(custom_timestamp) 
                | 'Form Key Value pair' >> beam.Map(lambda elements : (elements[0], int(elements[7])))
                | 'Window' >> beam.WindowInto(window.SlidingWindows(30,10))
                | 'Sum values' >> beam.CombinePerKey(sum)
                | 'Encode to byte string' >> beam.Map(encode_byte_string)
                | 'Write to pus sub' >> beam.io.WriteToPubSub(output_topic)



#              p
#               | 'Read from pub sub' >> #beam.io.ReadFromPubSub(subscription=input_subscription)
#               | 'Write to pub sub' >> beam.io.WriteToPubSub(output_topic)
              )

result = p.run()
result.wait_until_finish()

