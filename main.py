from neo4j_driver.driver import Neo4jDriver
from neo4j import GraphDatabase
from fastapi import FastAPI
import uvicorn
from starlette.responses import JSONResponse
import os
from dotenv import load_dotenv

app = FastAPI()


@app.get("/")
async def entry():
    return JSONResponse(status_code=200, content="Server works!")


@app.get("/all")
async def get_all():
    query = """
        MATCH (n)-[r]->(m)
        RETURN n, r, m
        """
    result = driver.query(query)
    return JSONResponse(status_code=200, content=result)


@app.post("/category/create")
async def create_category(name: str):
    query = (
        "CREATE (n:Person {name: $name})"
        "RETURN id(n) AS node_id, n.name AS node_name"
    )


if __name__ == "__main__":
    if os.getenv("INSIDE_DOCKER") is not None:
        username = os.getenv("USER")
        password = os.getenv("PASS")
        uri = os.getenv("URI")
    else:
        load_dotenv()
        username = os.getenv("USER")
        password = os.getenv("PASS")
        uri = os.getenv("URI")

    driver = Neo4jDriver(uri=uri, username=username, password=password)

    uvicorn.run(app, host="0.0.0.0", port=7000)
