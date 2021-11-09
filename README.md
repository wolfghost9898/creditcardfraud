# Analisis de fraudes por tarjeta de credito

# Azure Cosmos DB
 
 Se utilizo azure cosmos db como motor para la base de datos de grafos. [Azure Cosmos DB](https://azure.microsoft.com/es-mx/free/cosmos-db/search/?&ef_id=CjwKCAiA1aiMBhAUEiwACw25Mbpheq9knUvel2ayV1Qjy0Bkj2n6afCteE07w5s2hPohLe44PbG5LhoC3s0QAvD_BwE:G:s&OCID=AID2201055_SEM_CjwKCAiA1aiMBhAUEiwACw25Mbpheq9knUvel2ayV1Qjy0Bkj2n6afCteE07w5s2hPohLe44PbG5LhoC3s0QAvD_BwE:G:s&gclid=CjwKCAiA1aiMBhAUEiwACw25Mbpheq9knUvel2ayV1Qjy0Bkj2n6afCteE07w5s2hPohLe44PbG5LhoC3s0QAvD_BwE)

# Datos de Prueba
Los datos de prueba que se utilizaron fueron extraidos de la plataforma Kaggle. [Dataset](https://www.kaggle.com/kartik2112/fraud-detection)

# Transformacion de datos
Para la transformacion de datos se utilizara el archivo de python ``` dataConverter/dataConverter.py```, convirtiendo el dataset en informacion de utilidad

# Carga de Datos
Se utilizara el SDK de Azure Cosmos DB, para python para cargar la informacion en nuestra base de datos de grafos, se utilizara el archivo ``` uploadData/uploadData.py ```

# Consultas
## Listar Personas
```
    g.V().hasLabel("Person")
```

## Listar Lugares de Compras
```
    g.V().hasLabel("Merchant")
```

## Posibles Victimas de fraude
```
g.E().
hasLabel('HAS_BOUGHT_AT').
has('status', 'Disputed').as('disputed').
outV().as('victim').
outE('HAS_BOUGHT_AT').has('status','Undisputed').as('undisputed').
where(__.select('undisputed').where('undisputed',lt('disputed')).by('fecha')).as("t").inV().as("merchants").select("merchants","t","victim").
project("Victim","Merchants", "Amount","Time").
  by(__.select('victim').by('name')).
  by(__.select('merchants').by('name')).
  by(__.select('undisputed').by('amount')).
  by(__.select('undisputed').by('fecha')).dedup()

```
## Lugar de Origen de fraude
```
    g.E(). 
hasLabel('HAS_BOUGHT_AT').has('status', 'Disputed').as('disputed'). 
outV().as('victim'). 
outE('HAS_BOUGHT_AT').has('status', 'Undisputed').as('undisputed').
where(__.select('undisputed').where('undisputed', lt('disputed')).
by('fecha')).as("t").inV().as("merchants").
select("merchants","t","victim").group().
    by(__.select('merchants'))).
    by(__.fold().project('Suspicious_Store', 'Count', 'Victims').
        by(__.unfold().select('merchants')).
        by(__.unfold().select('t').dedup().count()).
        by(__.unfold().select('victim').dedup().fold())).
unfold().select(values).dedup()
```
## 
# Librerias utilizadas
```bash
    pip install azure-cosmos
```