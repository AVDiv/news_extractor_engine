from typing import Union

from fastapi import FastAPI

api_app = FastAPI(
    title="News Extractor Engine - Settings API",
    description="API for the News Extractor Engine settings.",
    version="0.1.0",
)


@api_app.get("/")
def read_root():
    return {"Hello": "World"}


@api_app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}
