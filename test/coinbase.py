from flask import Flask, jsonify
import json
import time
import requests
import schedule
import threading

app = Flask(__name__)

url = 'https://api.coinbase.com/v2/prices/ETH-USD/spot'
token = 'abd90df5f27a7b170cd775abf89d632b350b7c1c9d53e08b340cd9832ce52c2c'
headers = {'Authorization': f'Bearer {token}'}

def get_bitcoin_price():
    try:
        response = requests.get(url, headers=headers)
        data = response.json()
        return data
    except Exception as e:
        print("Error fetching Bitcoin price:", e)
        return None

def send_bitcoin_data():
    bitcoin_data = get_bitcoin_price()
    if bitcoin_data:
        print("sent")
        return bitcoin_data
    else:
        return {'error': 'Failed to fetch Bitcoin price.'}

@app.route('/start_stream')
def start_stream():
    return jsonify(send_bitcoin_data())

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    threading.Thread(target=run_scheduler, daemon=True).start()
    app.run(debug=True)

