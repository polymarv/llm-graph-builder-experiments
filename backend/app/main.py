import os, logging
from fastapi import FastAPI
from app.routes.pipeline import router as pipeline_router

app = FastAPI(title="LLMGB Backend (strict baseline)")
app.include_router(pipeline_router)

# Logging-Level aus ENV
logging.getLogger().setLevel(os.getenv("LOG_LEVEL", "INFO"))

@app.get("/health")
def health():
    return {"ok": True}
