from fastapi import FastAPI
from app.api.routers import router

app = FastAPI()

app.include_router(router)