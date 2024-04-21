from kafka import KafkaProducer
import requests
import time
from json import dumps

# Define the Kafka broker address and topics
bootstrap_servers = 'localhost:9092'
topic1 = 'topic1'
topic2 = 'topic2'
topic3 = 'topic3'

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: dumps(v).encode('utf-8'))

def get_crypto_assets():
    # Define the API endpoint
    url = 'https://api.coincap.io/v2/assets'

    while True:
        try:
            # Send GET request to the API endpoint
            response = requests.get(url)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Parse JSON response
                data = response.json()

                # Extract all attributes
                all_attributes = data['data'][0]

                # Publish all attributes to topic1
                print("All Attributes\n",all_attributes)
                producer.send(topic1, value=all_attributes)
                
                # Publish subset of attributes to topic2
                subset1 = {k: all_attributes[k] for k in ("id", "rank", "symbol", "name", "supply", "maxSupply", "marketCapUsd")}
                print("\n\nSubset 1\n",subset1)
                producer.send(topic2, value=subset1)

                # Publish rest of the attributes to topic3
                subset2 = {k: all_attributes[k] for k in ("id","name","volumeUsd24Hr", "priceUsd", "changePercent24Hr", "vwap24Hr")}
                print("\n\nSubset 2\n",subset2)
                producer.send(topic3, value=subset2)

            else:
                # Print error message if request was not successful
                print(f"Error: {response.status_code}")

        except Exception as e:
            # Print error message if an exception occurs
            print(f"Error: {e}")

        # Wait for 1 second before making the next request
        time.sleep(1)

# Call the function to get crypto assets
get_crypto_assets()
