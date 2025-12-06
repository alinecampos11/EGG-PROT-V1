from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import time

app = FastAPI()

# Cambia esto por el dominio real de tu frontend en Netlify cuando lo tengas
origins = [
    "http://localhost:5500",          # para pruebas locales
    "http://localhost:5173",          # otro puerto típico de front
    "https://TU-SITIO.netlify.app",   # <-- pon aquí tu URL de Netlify
    "*",                              # opcional: permitir todo (solo para pruebas)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class DataPoint(BaseModel):
    """
    Estructura de los datos que envían los ESP32.
    Ejemplo de JSON:
    {
      "device": "A",
      "value": 512,
      "t": 1712345678.12   # opcional, timestamp en segundos
    }
    """
    device: str          # "A" para persona A, "B" para persona B, etc.
    value: float         # lectura ADC o dato del sensor
    t: float | None = None  # timestamp opcional; si no viene, se genera en el servidor


# ---------- MANEJADOR DE CONEXIONES WEBSOCKET ----------

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast_json(self, message: dict):
        # Enviar el mismo mensaje a todos los clientes conectados
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                # Si falla, cerramos esa conexión
                self.disconnect(connection)


manager = ConnectionManager()


# ---------- RUTAS HTTP ----------

@app.get("/health")
async def health():
    """
    Ruta simple para comprobar que el backend está vivo.
    """
    return {"status": "ok"}


@app.post("/data")
async def receive_data(point: DataPoint):
    """
    Ruta a la que enviarán datos los ESP32 (via HTTP POST).

    Ejemplo desde un ESP32:
    POST /data
    {
      "device": "A",
      "value": 512
    }
    """
    # Si no vino timestamp desde el dispositivo, lo ponemos nosotros
    if point.t is None:
        point.t = time.time()

    msg = {
        "device": point.device,
        "value": point.value,
        "t": point.t,
    }

    # Enviar a todos los clientes WebSocket (tu página en Netlify)
    await manager.broadcast_json(msg)

    return {"ok": True}


# ---------- WEBSOCKET PARA EL FRONTEND ----------

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket que usará el frontend (Netlify) para recibir datos en tiempo real.
    El backend solo ENVÍA datos cuando recibe POST /data,
    aquí casi no esperamos mensajes desde el navegador.
    """
    await manager.connect(websocket)
    try:
        while True:
            # Si quieres, puedes escuchar mensajes desde el front:
            # data = await websocket.receive_text()
            # pero para este prototipo solo hacemos un "ping" para
            # mantener viva la conexión.
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)
