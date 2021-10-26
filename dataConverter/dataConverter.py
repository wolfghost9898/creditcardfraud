import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import os

dirname = os.getcwd()
dataSet = pd.read_csv(dirname + "/input/data.csv")


verticePerson = open(dirname + "/output/vertext_Person.txt","w")


dataSet["full_name"] = dataSet["first"] + "_" + dataSet["last"] + "#" + dataSet["gender"]
for row in dataSet["full_name"].unique():
    personID = row.split("#")[0]
    name = personID.split("_")[0] + " " + personID.split("_")[1]
    gender = row.split("#")[1]
    query = "g.addV('Person').property('id','" + personID + "').property('name','" + name + "').property('gender','" + gender + "')\n"
    verticePerson.write(query)
verticePerson.close()



edgesTransactions = open(dirname + "/output/edges_transactions.txt","w")

id = 1
for i in range(len(dataSet.values)):
    fraud = ("Disputed","Undisputed")[dataSet["is_fraud"].values[i] == 0]
    fecha = dataSet["trans_date_trans_time"].values[i]
    merchant = dataSet["merchant"].values[i]
    merchant = merchant.replace(",", " ")
    merchant = merchant.replace("fraud_", "")
    amount = dataSet["amt"].values[i]
    personID = dataSet["full_name"].values[i].split("#")[0]
    name = personID.split("_")[0] + " " + personID.split("_")[1]
    query = "g.V('" + personID + "').addE('HAS_BOUGHT_AT').to(g.V('" + merchant +"')).property('amount','" + str(amount) + "').property('fecha','" + fecha + "').property('status','" + fraud +"')\n"
    edgesTransactions.write(query)
    id += 1

edgesTransactions.close()


verticeMerchant = open(dirname + "/output/vertext_Merchant.txt","w")
dataSet["full_merchant"] = dataSet["merchant"] + "#" + dataSet["street"] + "#" + dataSet["city"]
for row in dataSet["full_merchant"].unique():
    merchant = row.split("#")[0]
    merchant = merchant.replace(",", " ")
    merchant = merchant.replace("fraud_", "")
    street = row.split("#")[1]
    city = row.split("#")[2]
    query = "g.addV('Merchant').property('id','" + merchant + "').property('name','" + merchant + "').property('street','" + street + "').property('city','" + city + "')\n"
    verticeMerchant.write(query)
    
verticeMerchant.close()