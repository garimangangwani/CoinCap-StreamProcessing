from kafka import KafkaProducer
import requests
import time
from json import dumps

# Define the Kafka broker address and topic
bootstrap_servers = 'localhost:9092'
topic = 'crypto_assets'

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

                # Extract relevant data (e.g., asset names, symbols, prices, and timestamp)
                assets = data['data']
                timestamp = data['timestamp']
                
                first_asset = data['data'][0]
                timestamp = data['timestamp']
                name = first_asset['name']
                symbol = first_asset['symbol']
                price = first_asset['priceUsd']
                
                # Create a dictionary containing the extracted data
                asset_data = {
                    'timestamp': timestamp,
                    'name': name,
                    'symbol': symbol,
                    'price': price
                }
                
                # Send the data to the Kafka topic
                producer.send(topic, value=asset_data)
                print("Sent to Kafka:", asset_data)

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
