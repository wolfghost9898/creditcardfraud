from gremlin_python.driver import client, serializer, protocol
from gremlin_python.driver.protocol import GremlinServerError
import sys
import traceback
import config
import os

dirname = os.getcwd()

ENDPOINT = config.cosmosDB['ENDPOINT']
DATABASE = config.cosmosDB['DATABASE']
COLLECTION = config.cosmosDB['COLLECTION']
PRIMARY_KEY = config.cosmosDB['PRIMARY_KEY']


def readFile(path):
    fileObject = open(path,"r")
    rows = fileObject.read().splitlines()
    fileObject.close()
    return rows

def insertar_vertices(gremlin_client,VERTICES):
    i = 0
    for vertex in VERTICES:
        callback = gremlin_client.submitAsync(vertex)
        if callback.result() is not None:
            i += 1
        else:
            print("Error: {0}".format(vertex))
    print("Se insertaron {0} vertices".format(i))


def insertar_edges(gremlin_client,EDGES):
    i = 0
    for edge in EDGES:
        callback = gremlin_client.submitAsync(edge)
        if callback.result() is not None:
            i += 1
        else:
            print("Error: {0}".format(edge))
    print("Se insertaron {0} edges".format(i))

def cleanAll(gremlin_client):
    callback = gremlin_client.submitAsync("g.V().drop()")
    if callback.result() is not None:
        print("Grafo Vacio!")

verticesPersonas = readFile(dirname + "/output/vertext_Person.txt")
verticesMercancia = readFile(dirname + "/output/vertext_Merchant.txt")
edgesVentas = readFile(dirname + "/output/edges_transactions.txt")


try:
    print('Conectando con gremlin')
    gremlin_client = client.Client(
        ENDPOINT , 'g',
        username="/dbs/" + DATABASE + "/colls/" + COLLECTION,
        password=PRIMARY_KEY,
        message_serializer=serializer.GraphSONSerializersV2d0()
    )
    print('Conexion exitosa')
    cleanAll(gremlin_client)
    print('Insertando vertices de personas')
    insertar_vertices(gremlin_client,verticesPersonas)

    print('Insertando vertices de mercancia')
    verticesMercancia = verticesMercancia[:1000]
    insertar_vertices(gremlin_client,verticesMercancia)

    edgesVentas = edgesVentas[1000:2000]
    
    insertar_edges(gremlin_client,edgesVentas)

except GremlinServerError as e:
    print('Code: {0}, Attributes: {1}'.format(e.status_code, e.status_attributes))
    cosmos_status_code = e.status_attributes["x-ms-status-code"]
    if cosmos_status_code == 409:
        print('Conflict error!')
    elif cosmos_status_code == 412:
        print('Precondition error!')
    elif cosmos_status_code == 429:
        print('Throttling error!');
    elif cosmos_status_code == 1009:
        print('Request timeout error!')
    else:
        print("Default error handling")

    traceback.print_exc(file=sys.stdout) 
    sys.exit(1)
