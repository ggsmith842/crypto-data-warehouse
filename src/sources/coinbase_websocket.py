import asyncio
import json
import websockets




async def websocket_listener():

    URI = 'wss://ws-feed.exchange.coinbase.com'

    subscribe_message = {
    "type": "subscribe",
    "product_ids": ["BTC-USD"],
    "channels": ["ticker"]
    }

    while True:
        try:
            async with websockets.connect(URI, ping_interval=None) as websocket:
                await websocket.send(json.dumps(subscribe_message))
                while True:
                    response = await websocket.recv()
                    json_response = json.loads(response)
                    print(json_response)

        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK):
            print('Connection closed, retrying..')
            await asyncio.sleep(1)

if __name__ == '__main__':
    try:
        asyncio.run(websocket_listener())
    except KeyboardInterrupt:
        print("Exiting WebSocket..")