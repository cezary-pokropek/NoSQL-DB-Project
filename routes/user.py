from fastapi import APIRouter
from models.user import Book 
from config.db import conn 
from schemas.user import serializeDict, serializeList
from bson import ObjectId
book = APIRouter() 

@book.get('/')
async def find_all_books():
    return serializeList(conn.local.books.find())

@book.get('/{id}')
async def find_one_book(id):
    return serializeDict(conn.local.books.find_one({"_id":ObjectId(id)}))

@book.post('/')
async def create_book(book: Book):
    conn.local.books.insert_one(dict(book))
    return serializeList(conn.local.books.find())

@book.put('/{id}')
async def update_book(id,book: Book):
    conn.local.books.find_one_and_update({"_id":ObjectId(id)},{"$set":dict(book)})
    return serializeDict(conn.local.books.find_one({"_id":ObjectId(id)}))

@book.delete('/{id}')
async def delete_book(id):
    return serializeDict(conn.local.books.find_one_and_delete({"_id":ObjectId(id)}))