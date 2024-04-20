import requests
import json
import time

def receive_streaming_data(url):
    try:
        # Send a GET request to the server
        response = requests.get(url, stream=True)

        # Check if the request was successful
        if response.status_code == 200:
            # Print the raw response
            print("Raw Response:")
            print(response.text)

            # Iterate over the lines of the response, which should contain JSON data
            for line in response.iter_lines():
                # Decode the JSON data from the response line
                if line:
                    data = json.loads(line)
                    # Process the received data (e.g., print or save to a file)
                    print(data)
        else:
            print(f"Error: {response.status_code}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # URL of the Flask server
    server_url = 'http://localhost:5000/start_stream'
    
    # Call the function to receive streaming data
    receive_streaming_data(server_url)

