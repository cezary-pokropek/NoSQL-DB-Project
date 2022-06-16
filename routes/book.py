from fastapi import APIRouter
# from models.book import Book 
from config.db import conn 
from schemas.book import serializeDict # , serializeList
from bson import ObjectId
book = APIRouter() 

@book.get('/{book_id}')
async def find_book_by_id(book_id):
    return serializeDict(conn.bgd.books.find_one({"_id":ObjectId(book_id)}))

@book.get('/{author_id}')
async def find_author_by_id(author_id):
    return serializeDict(conn.bgd.authors.find_one({"_id":ObjectId(author_id)}))