from fastapi import FastAPI, WebSocket
from starlette.websockets import WebSocketDisconnect
import uvicorn

app = FastAPI()

# Store active WebSocket connections
connections = []

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connections.append(websocket)
    print(connections)
    try:
        while True:
            # In this example, the server waits for a message before sending updates
            # This can be replaced or supplemented with your own logic to push updates
            data = await websocket.receive_text()
            # print(f"Data received: {data}")
            # Iterate over connections to broadcast the message, excluding the sender
            for connection in connections:
                if connection != websocket:
                    await connection.send_text(data)
    except WebSocketDisconnect:
        connections.remove(websocket)
        print("Client disconnected")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
