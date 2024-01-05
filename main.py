from starlette.middleware.cors import CORSMiddleware

from neo4j_driver.driver import Neo4jDriver
from neo4j import GraphDatabase
from fastapi import FastAPI
import uvicorn
from starlette.responses import JSONResponse
import os
from dotenv import load_dotenv

app = FastAPI()

origins = [
    "http://localhost:3000",
    "https://localhost:3000",
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def entry():
    return JSONResponse(status_code=200, content="Server works!")


@app.get("/all")
async def get_all():
    query = """
            MATCH (n)-[r]->(m)
            OPTIONAL MATCH (n)<-[r2]-(upperNode)
            WITH n, COLLECT(DISTINCT m) AS upper, COLLECT(DISTINCT upperNode) AS under_nodes
            RETURN n.name AS node_name, 
                   CASE WHEN SIZE(upper) > 0 THEN upper[0].name ELSE NULL END AS upper_node,
                   CASE WHEN n.labels <> "Category" THEN "has_info" ELSE NULL END AS has_info,
                   under_nodes
            """
    result = driver.query(query)

    formatted_result = [
        {
            "upper_node": None,
            "under_nodes": [],
            "name": "Base",
            "hasInformation": False
        }
    ]
    for record in result:
        node_name = record["node_name"]
        upper_node = record["upper_node"]
        under_nodes = record["under_nodes"]
        has = record["has_info"]

        if upper_node == "Base":
            for index, node in enumerate(formatted_result):
                if node["name"] == "Base":
                    formatted_result[index]["under_nodes"].append(node_name)
                    break

        formatted_result.append({
            "name": node_name,
            "upper_node": upper_node,
            "under_nodes": under_nodes,
            "hasInformation": True if has == "has_info" else False
        })

    return JSONResponse(status_code=200, content=formatted_result)


@app.post("/category/create")
async def create_category(name: str):
    query = (
        "MERGE(m:MainNode {name: 'Base'})"
        "MERGE (n:Category {name: $name})-[:BELONGS_TO]->(m)"
        "RETURN id(n) AS node_id, n.name AS node_name"
    )

    try:
        driver.query(query, parameters={"name": name}, fetch_one=True)
    except:
        return JSONResponse(status_code=500, content="An error occurred when creating the category!")

    return JSONResponse(status_code=200, content="Category created successfully!")


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
