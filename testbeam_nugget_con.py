# Process
from __future__ import print_function
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount
from beam_nuggets.io import kafkaio

#with beam.Pipeline(options=PipelineOptions()) as p:
#    notifications = (p
#                     | "Creating data" >> beam.Create([('dev_1', '{"device": "0001", status": "healthy"}')])
#                     | "Pushing messages to Kafka" >> kafkaio.KafkaProduce(
#                                                                            topic='ORIG',
#                                                                            servers="localhost:9092"
#                                                                        )
#                    )
#    notifications | 'Writing to stdout' >> beam.Map(print)

kafka_topic = "ORIG"
kafka_config = {"topic": kafka_topic,
                "bootstrap_servers": "localhost:9092",
                "group_id": "notification_consumer_group"}

with beam.Pipeline(options=PipelineOptions()) as p:
    notifications = p | "Reading messages from Kafka" >> kafkaio.KafkaConsume(kafka_config)
    notifications | 'Writing to stdout' >> beam.Map(print)




