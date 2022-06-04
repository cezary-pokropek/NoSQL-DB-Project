from fastapi import FastAPI
from routes.book import book 
app = FastAPI()
app.include_router(book)
