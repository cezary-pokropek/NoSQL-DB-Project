from fastapi import FastAPI
from routes.user import book 
app = FastAPI()
app.include_router(book)
