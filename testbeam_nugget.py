# Process
from __future__ import print_function
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount

#import apache_beam as beam
#from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import kafkaio


