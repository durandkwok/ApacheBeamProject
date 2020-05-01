# Subscribe
from google.cloud import pubsub_v1
import time
import os

if __name__ == "__main__":
  # Replace with your service account path
#  os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'home/dkwok/gcp-se-fe24e05febf1.json'
  os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'gcp-se-fe24e05febf1.json'

  # Replace with your subscription id
  subscription_path = 'projects/gcp-se/subscriptions/Subscribe2test'

  subscriber = pubsub_v1.SubscriberClient()

  def callback(message):
    print(('Received message:{}'.format(message)))
    message.ack()

  subscriber.subscribe(subscription_path, callback=callback)
  while True:
    time.sleep(60)

