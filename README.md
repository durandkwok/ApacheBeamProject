# BeamProject

Project of testing flexible pipeline with Apache Beam. This project also utilize beam_nugget which has a library for Apache Kafka as well as Google Cloud Platform SDK.


from google.cloud import pubsub_v1
import apache_beam as beam
from beam_nuggets.io import kafkaio


publish.py - demonstrates the ability to integrate with google pub/sub and Kafka. Please see code snippet below.

For example:

"publisher.publish(pubsub_topic, event_data)"

and

"with beam.Pipeline(options=PipelineOptions()) as p:\
notifications = (p
| "Creating data" >> beam.Create([('dev_1', '{"device": "0001", status": "healthy"}')])
| "Creating data" >> beam.Create([('Kafka:',event_data )])
| "Pushing messages to Kafka" >> kafkaio.KafkaProduce(topic='ORIG', servers="localhost:9092")"


process.py or processSlide.py - demonstrates the use of sliding window vs tumbling window then write to Goolge Pub/Sub
For example:

"| 'Window' >> beam.WindowInto(window.SlidingWindows(30,10))
| 'Sum values' >> beam.CombinePerKey(sum) # STR_2 , []
| 'Encode to byte string' >> beam.Map(encode_byte_string)
| 'Write to pus sub' >> beam.io.WriteToPubSub(output_topic)"

subscribe.py - demonstrates the use of subscribing from GCP Pub/Sub

For example:

"subscription_path = 'projects/gcp-se/subscriptions/Subscribe2test'
subscriber = pubsub_v1.SubscriberClient()
subscriber.subscribe(subscription_path, callback=callback)"


