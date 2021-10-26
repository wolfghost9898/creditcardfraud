from gremlin_python.driver import client
import config

ENDPOINT = config.cosmosDB['ENDPOINT']
DATABASE = config.cosmosDB['DATABASE']
COLLECTION = config.cosmosDB['COLLECTION']
PRIMARY_KEY = config.cosmosDB['PRIMARY_KEY']