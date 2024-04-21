#python3 coincap_producer.py bitcoin ethereum dogecoin
from kafka import KafkaProducer
import requests
import json
import sys
import time

# Define the Kafka broker address
bootstrap_servers = 'localhost:9092'

coin1=sys.argv[1]
coin2=sys.argv[2]
coin3=sys.argv[3]

# Define the URLs and corresponding topics
urls = {
    'https://api.coincap.io/v2/assets/'+coin1: coin1,
    'https://api.coincap.io/v2/assets/'+coin2: coin2,
    'https://api.coincap.io/v2/assets/'+coin3: coin3
}

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_and_publish_data():
    print("Topics:","\n",coin1,"\n",coin2,"\n",coin3,"\n")
    while True:
        try:
            # Iterate over the URLs and corresponding topics
            for url, topic in urls.items():
                # Send GET request to the API endpoint
                response = requests.get(url)

                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    # Parse JSON response
                    data = response.json()['data']
                    
                    # Convert specific fields from string to float or double
                    for key in ['supply', 'maxSupply', 'marketCapUsd', 'volumeUsd24Hr', 'priceUsd', 'changePercent24Hr', 'vwap24Hr']:
                        if key in data and data[key] is not None:
                            data[key] = float(data[key])

                    # Publish data to Kafka topic
                    producer.send(topic, value=data)
                    print(f"Data published to Kafka topic {topic}: {data}")

                else:
                    # Print error message if request was not successful
                    print(f"Error: {response.status_code}")

        except Exception as e:
            # Print error message if an exception occurs
            print(f"Error: {e}")

        # Wait for 5 seconds before making the next request
        time.sleep(5)

# Call the function to fetch and publish data
fetch_and_publish_data()

