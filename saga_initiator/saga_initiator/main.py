from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum

from saga_initiator.routes.purchase import router as purchase_router
from saga_initiator.routes.status import router as status_router

app = FastAPI(
    title="Flash Sale Saga — Initiator API",
    version="0.1.0",
    description="Saga Initiator API for the Flash Sale distributed ticketing platform",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(purchase_router, tags=["purchase"])
app.include_router(status_router, tags=["status"])


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


handler = Mangum(app)
