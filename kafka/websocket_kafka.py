import websocket
import threading
import json
from kafka import KafkaProducer

# Kafka Producer setup
producer = KafkaProducer(bootstrap_servers=['0.0.0.0:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def on_message(ws, message):
    print(json.loads(message))
    try:
        # Send message to Kafka topic
        producer.send('coinbase_feed', value=message)
        producer.flush()
    except Exception as e:
        print("Error sending message to Kafka:", e)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    def run(*args):
        subscribe_message = {
            "type": "subscribe",
            "product_ids": ["BTC-USD", "ETH-USD"],
            "channels": ["ticker_batch"]
        }
        ws.send(json.dumps(subscribe_message) + "\n")
    threading.Thread(target=run).start()

if __name__ == "__main__":
    websocket.enableTrace(True)
    
    ws = websocket.WebSocketApp("wss://ws-feed.exchange.coinbase.com",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
