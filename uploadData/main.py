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

def cleanAll(gremlin_client):
    callback = gremlin_client.submitAsync("g.V().drop()")
    if callback.result() is not None:
        print("Grafo Vacio!")

verticesPersonas = readFile(dirname + "/output/vertext_Person.txt")


try:
    print('Conectando con gremlin')
    gremlin_client = client.Client(
        ENDPOINT , 'g',
        username="/dbs/" + DATABASE + "/colls/" + COLLECTION,
        password=PRIMARY_KEY,
        message_serializer=serializer.GraphSONSerializersV2d0()
    )
    cleanAll(gremlin_client)
    print('Conexion exitosa')
    verticesPersonas = verticesPersonas[:1]
    insertar_vertices(gremlin_client,verticesPersonas)

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
