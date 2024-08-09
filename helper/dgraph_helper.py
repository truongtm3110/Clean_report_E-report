# Create a client stub.
import pydgraph


# Create a client stub
def create_client_stub(host='localhost', port=9080):
    return pydgraph.DgraphClientStub(f'{host}:{port}')


# Create a client
def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


# Drop All - discard all data and start from a clean slate
def drop_all(client: pydgraph.DgraphClient):
    return client.alter(pydgraph.Operation(drop_all=True))


# Set schema.
def set_schema(client: pydgraph.DgraphClient, schema: str):
    return client.alter(pydgraph.Operation(schema=schema))

