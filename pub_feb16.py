import os
import time 
from google.cloud import pubsub_v1
if __name__ == "__main__":
       # Replace  with your project id
    project = 'gcp-se'

    # Replace  with your pubsub topic
    pubsub_topic = 'projects/gcp-se/topics/Topic1test'

    # Replace with your service account path
#    path_service_account = '/home/dkwok/gcp-se-fe24e05febf1.json'
    path_service_account = 'gcp-se-fe24e05febf1.json'

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account    

# Read Transform
# ReadFromParquet(file_pattern, min_bundle_size, validate, columns(list[str]) )
# ReadFromAvro()
# ReadFromTFRecord(file_pattern, validate, compression_type, coder)

# Queue Transform
# kafka, kinesis, jms, mqtt, pubsub

# ReadFromPubsub(
#    topic(str), subscription(str), id_label(str), with_attribute(boolean)
#, timestamp_attribute(int) specify the value to be used as element timestamp
#)

# DB Read Transform
# Cassandra, HBase, Kudu, BQ, MongoDB, Redis, Google Cloud DataStore


    # Replace  with your input file path
#    input_file = '/home/dkwok/store_sales.csv'
    input_file = 'store_salesNoTime.csv'
#    input_file = 'store_sales.csv'

    # create publisher
    publisher = pubsub_v1.PublisherClient()
    with open(input_file, 'rb') as ifp:
        # skip header
        header = ifp.readline()  
        
        # loop over each record
        for line in ifp:
            event_data = line   # entire line of input CSV is the message
            print('Publishing {0} to {1}'.format(event_data, pubsub_topic))
            publisher.publish(pubsub_topic, event_data)
            time.sleep(1)   

