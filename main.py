from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
from neo4j_driver.driver import Neo4jDriver
from fastapi import FastAPI
import uvicorn
from starlette.responses import JSONResponse
import os
from typing import Dict
from dotenv import load_dotenv


class Dataset(BaseModel):
    name: str
    belongs_to: str
    url: str
    tags: Dict[str, str]
    user: str
    description: str


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
                   CASE WHEN n.url <> "" THEN "has_info" ELSE NULL END AS has_info,
                   under_nodes,
                   labels(n) as label
            """
    result = driver.query(query)

    formatted_result = [
        {
            "upper_node": None,
            "under_nodes": [],
            "name": "Base",
            "label": "base",
            "hasInformation": False
        }
    ]
    for record in result:
        node_name = record["node_name"]
        upper_node = record["upper_node"]
        under_nodes = record["under_nodes"]
        has = record["has_info"]
        label = record["label"][0]

        if str(upper_node).lower() == "base":
            for index, node in enumerate(formatted_result):
                if node["name"] == "Base":
                    formatted_result[index]["under_nodes"].append(node_name)
                    break

        if len(under_nodes) > 0:
            formatted_under_nodes = []
            for under_node in under_nodes:
                formatted_under_nodes.append(under_node["name"])

            formatted_result.append({
                "name": node_name,
                "upper_node": upper_node,
                "under_nodes": formatted_under_nodes,
                "label": label.lower(),
                "hasInformation": True if has == "has_info" else False
            })
        else:
            formatted_result.append({
                "name": node_name,
                "upper_node": upper_node,
                "under_nodes": under_nodes,
                "label": label.lower(),
                "hasInformation": True if has == "has_info" else False
            })

    return JSONResponse(status_code=200, content=formatted_result)


@app.get("/categories")
async def get_categories():
    query = """
            MATCH (n:Category)
            RETURN n.name as node_name
            """
    result = driver.query(query)

    formatted_result = []

    for record in result:
        node_name = record["node_name"]

        formatted_result.append(node_name.capitalize())

    return JSONResponse(status_code=200, content=formatted_result)


@app.post("/category/create")
async def create_category(name: str):
    query = (
        "MERGE(m:MainNode {name: 'Base'})"
        "MERGE (n:Category {name: $name})-[:BELONGS_TO]->(m)"
        "RETURN id(n) AS node_id, n.name AS node_name"
    )

    result = driver.query(query, parameters={"name": name}, fetch_one=True)
    if not result:
        return JSONResponse(status_code=500, content="An error occurred when creating the category!")

    return JSONResponse(status_code=201, content="Category created successfully!")


@app.delete("/category/delete")
async def delete_category(name: str):
    query = (
        "MATCH (n: Category {name: $name}) "
        "DETACH DELETE n "
        "RETURN id(n) AS node_id"
    )

    result = driver.query(query, parameters={"name": name}, fetch_one=True)
    if not result:
        return JSONResponse(status_code=500, content="An error occurred when deleting the category!")

    return JSONResponse(status_code=201, content="Category deleted successfully!")


@app.get("/dataset")
async def get_dataset(name: str):
    query = (
        "MATCH (n: Dataset {name: $name}) "
        "RETURN n as n"
    )

    result = driver.query(query, parameters={"name": name}, fetch_one=True)
    if not result:
        return JSONResponse(status_code=500, content="An error occurred when getting the dataset!")

    formatted_result = {}
    for record in result:
        for k, v in record.items():
            formatted_result[k] = v

    return JSONResponse(status_code=200, content=formatted_result)


@app.post("/dataset/create")
async def create_dataset(dataset: Dataset):
    query = (
        "MERGE(m:Category {name: $belonging}) "
        "MERGE (n:Dataset {name: $name, url: $url, user: $user, description: $description})-[:BELONGS_TO]->(m) "
        "SET n += $properties "
        "RETURN id(n) AS node_id, n.name AS node_name"
    )

    result = driver.query(query, parameters={"name": dataset.name.lower(),
                                             "belonging": dataset.belongs_to.lower(),
                                             "url": dataset.url,
                                             "user": dataset.user,
                                             "description": dataset.description,
                                             "properties": dataset.tags},
                          fetch_one=True)
    if not result:
        return JSONResponse(status_code=500, content="An error occurred when creating the category!")

    return JSONResponse(status_code=201, content="Dataset created successfully!")


@app.delete("/dataset/delete")
async def delete_dataset(name: str):
    query = (
        "MATCH (n: Dataset {name: $name}) "
        "DETACH DELETE n "
        "RETURN id(n) AS node_id"
    )

    result = driver.query(query, parameters={"name": name}, fetch_one=True)
    if not result:
        return JSONResponse(status_code=500, content="An error occurred when deleting the dataset!")

    return JSONResponse(status_code=201, content="Dataset deleted successfully!")


@app.get("/datasets")
async def get_datasets(user: str):
    query = (
        "MATCH (n:Dataset {user: $user})"
        "RETURN n AS n"
    )

    result = driver.query(query, parameters={"user": user})
    if not result:
        return JSONResponse(status_code=500, content="An error occurred when getting the datasets!")

    formatted_result = []

    for record in result:
        tags = {}
        for k, v in record["n"].items():
            tags[k] = v
        formatted_result.append(tags)

    return JSONResponse(status_code=201, content=formatted_result)


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
