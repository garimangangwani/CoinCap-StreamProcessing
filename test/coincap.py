import requests
import time

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
                
                #print(f"Timestamp: {timestamp}")
                #for asset in assets :
                 #   name = asset['name']
                  #  symbol = asset['symbol']
                   # price = asset['priceUsd']
                    #print(f"Name: {name}, Symbol: {symbol}, Price: {price}")
                first_asset = data['data'][0]
                timestamp = data['timestamp']
                name = first_asset['name']
                symbol = first_asset['symbol']
                price = first_asset['priceUsd']
                print(f"Timestamp: {timestamp}")
                print(f"Name: {name}, Symbol: {symbol}, Price: {price}")

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

