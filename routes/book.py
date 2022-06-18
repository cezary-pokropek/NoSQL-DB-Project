from fastapi import APIRouter
from config.db import conn 
from schemas.book import serializeDict
from bson import ObjectId
book = APIRouter() 

@book.get('/{book_id}')
async def find_book_by_id(book_id):
    return serializeDict(conn.bgd.books.find_one({"_id":ObjectId(book_id)}))
