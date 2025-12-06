from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict
import time

import numpy as np
import pywt  # Wavelet

app = FastAPI()

# ---------- CORS (para que Netlify pueda conectarse) ----------
origins = [
    "*",  # en producción puedes restringir a tu dominio Netlify
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------- MODELO DE DATOS QUE ENVÍAN LOS ESP32 ----------
class DataPoint(BaseModel):
    device: str          # "A" o "B"
    value: float         # lectura del sensor
    t: float | None = None  # timestamp opcional


# ---------- GESTOR DE CONEXIONES WEBSOCKET ----------
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
        for ws in list(self.active_connections):
            try:
                await ws.send_json(message)
            except Exception:
                self.disconnect(ws)


manager = ConnectionManager()

# ---------- BUFFERS DE SEÑAL PARA A Y B ----------
# Guardamos las últimas N muestras para cada dispositivo
buffers: Dict[str, List[float]] = {
    "A": [],
    "B": [],
}

WINDOW_SIZE = 256  # N muestras por ventana (asumiendo ~50 Hz = ~5 s)
FS = 50.0          # Frecuencia de muestreo aproximada (Hz)


# ---------- FUNCIÓN: CÁLCULO DE PLV CON WAVELET MORLET ----------
def compute_plv_wavelet(signal_a: np.ndarray,
                        signal_b: np.ndarray,
                        fs: float = FS,
                        target_freq: float = 10.0) -> float:
    """
    Calcula el PLV entre dos señales usando wavelet continua (Morlet)
    en una frecuencia objetivo (por ejemplo 10 Hz ~ banda alfa baja).
    """

    if len(signal_a) != len(signal_b):
        # Igualamos tamaños por seguridad
        n = min(len(signal_a), len(signal_b))
        signal_a = signal_a[-n:]
        signal_b = signal_b[-n:]

    # Escalas para CWT
    scales = np.arange(1, 64)

    # Wavelet continua (compleja)
    coef_a, freqs = pywt.cwt(signal_a, scales, "morl", sampling_period=1.0 / fs)
    coef_b, _ = pywt.cwt(signal_b, scales, "morl", sampling_period=1.0 / fs)

    # Elegimos el índice de la frecuencia más cercana a la target_freq
    idx = int(np.argmin(np.abs(freqs - target_freq)))

    phase_a = np.angle(coef_a[idx])
    phase_b = np.angle(coef_b[idx])

    phase_diff = phase_a - phase_b
    plv = np.abs(np.mean(np.exp(1j * phase_diff)))

    return float(plv)


# ---------- RUTAS HTTP ----------

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/data")
async def receive_data(point: DataPoint):
    """
    Ruta a la que enviarán datos los ESP32 (via HTTP POST).
    Ejemplo JSON:
    {
      "device": "A",
      "value": 512
    }
    """
    # normalizamos device a "A" o "B"
    dev = point.device.upper()

    if point.t is None:
        point.t = time.time()

    # Guardar muestra en el buffer correspondiente (si es A o B)
    if dev in buffers:
        buffers[dev].append(point.value)
        # Nos quedamos solo con las últimas WINDOW_SIZE muestras
        if len(buffers[dev]) > WINDOW_SIZE:
            buffers[dev] = buffers[dev][-WINDOW_SIZE:]

    # Mensaje base para el frontend
    msg = {
        "device": dev,
        "value": point.value,
        "t": point.t,
        "metrics": {}
    }

    # Si tenemos datos suficientes en ambos canales, calculamos PLV
    if len(buffers["A"]) >= WINDOW_SIZE and len(buffers["B"]) >= WINDOW_SIZE:
        sig_a = np.array(buffers["A"][-WINDOW_SIZE:])
        sig_b = np.array(buffers["B"][-WINDOW_SIZE:])

        try:
            plv_alpha = compute_plv_wavelet(sig_a, sig_b, fs=FS, target_freq=10.0)
            msg["metrics"]["plv_alpha"] = plv_alpha
        except Exception as e:
            # Si algo falla en el cálculo de wavelet / PLV, no rompemos todo
            msg["metrics"]["error"] = str(e)

    # Enviar a todos los clientes websocket (tu página en Netlify)
    await manager.broadcast_json(msg)

    return {"ok": True}


# ---------- WEBSOCKET PARA EL FRONTEND ----------

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # El servidor realmente no necesita recibir nada del front,
            # pero esto mantiene abierta la conexión.
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)
