# from typing import Optional
# from pydantic import BaseModel, Field

# class Book(BaseModel):
#     title: str = Field(min_length=1)
#     author: str = Field(min_length=1, max_length=250)
#     rating: int = Field(gt=-1, lt=101)

# class Book2(BaseModel):
#     title: str = Field(min_length=1)
#     dupa: str = Field(min_length=1, max_length=250)
#     rating: int = Field(gt=-1, lt=101)