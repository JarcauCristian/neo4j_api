from neo4j import GraphDatabase


class Neo4jDriver:
    def __init__(self, uri, username, password):
        self._driver = GraphDatabase.driver(uri, auth=(username, password))

    def close(self):
        self._driver.close()

    def query(self, query, parameters=None, fetch_one=False):
        with self._driver.session() as session:
            result = session.run(query, parameters)
            return result.single() if fetch_one else result.data()
