from kafka import KafkaProducer
import requests
import json
import time

# Define the Kafka broker address
bootstrap_servers = 'localhost:9092'

# Define the URLs and corresponding topics
urls = {
    'https://api.coincap.io/v2/assets/bitcoin': 'bitcoin_data',
    'https://api.coincap.io/v2/assets/ethereum': 'ethereum_data',
    'https://api.coincap.io/v2/assets/dogecoin': 'dogecoin_data'
}

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_and_publish_data():
    while True:
        try:
            # Iterate over the URLs and corresponding topics
            for url, topic in urls.items():
                # Send GET request to the API endpoint
                response = requests.get(url)

                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    # Parse JSON response
                    data = response.json()

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

