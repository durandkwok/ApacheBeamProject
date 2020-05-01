# Process
from __future__ import print_function
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount
from beam_nuggets.io import kafkaio

with beam.Pipeline(options=PipelineOptions()) as p:
    notifications = (p
                     | "Creating data" >> beam.Create([('dev_1', '{"device": "0001", status": "healthy"}')])
                     | "Pushing messages to Kafka" >> kafkaio.KafkaProduce(
                                                                            topic='IOT',
                                                                            servers="localhost:9092"
                                                                        )
                    )
    notifications | 'Writing to stdout' >> beam.Map(print)



