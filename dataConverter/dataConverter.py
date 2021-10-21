import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
dirname = os.getcwd()
dataSet = pd.read_csv(dirname + "/input/data.csv")


verticePerson = open(dirname + "/output/vertext_Person.txt","w")
verticePerson.write("~id,~label,name:String,gender:String\n")


dataSet["full_name"] = dataSet["first"] + " " + dataSet["last"] + "#" + dataSet["gender"]
for row in dataSet["full_name"].unique():
    name = row.split("#")[0]
    gender = row.split("#")[1]
    verticePerson.write(name + ", Person, " + name + "," + gender  + "\n")
verticePerson.close()



edgesTransactions = open(dirname + "/output/edges_transactions.txt","w")
edgesTransactions.write("~id,~from,~label,amount:Int,time:Date,status:String,~to\n")

id = 1
for i in range(len(dataSet.values)):
    fraud = ("Disputed","Undisputed")[dataSet["is_fraud"].values[i] == 0]
    fecha = dataSet["trans_date_trans_time"].values[i]
    merchant = dataSet["merchant"].values[i]
    merchant = merchant.replace(",", " ")
    merchant = merchant.replace("fraud_", "")
    amount = dataSet["amt"].values[i]
    name = dataSet["full_name"].values[i].split("#")[0]
    edgesTransactions.write("id" + str(id) + "," + name + ",HAS_BOUGHT_AT," + "," + str(amount) + "," + fecha + "," + fraud + "," + merchant + "\n")
    id += 1

edgesTransactions.close()


verticeMerchant = open(dirname + "/output/vertext_Merchant.txt","w")
verticeMerchant.write("~id,~label,name:String,street:String,address:String\n")
dataSet["full_merchant"] = dataSet["merchant"] + "#" + dataSet["street"] + "#" + dataSet["city"]
for row in dataSet["full_merchant"].unique():
    merchant = row.split("#")[0]
    merchant = merchant.replace(",", " ")
    merchant = merchant.replace("fraud_", "")
    street = row.split("#")[1]
    city = row.split("#")[2]
    verticeMerchant.write(merchant + ", Merchant, " + merchant + "," + street + "," + city  + "\n")
    
verticeMerchant.close()