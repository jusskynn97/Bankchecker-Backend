from fastapi import FastAPI
from mbbankchecker import MBBank

app = FastAPI()

@app.get("/")
def home():
    return "home page"