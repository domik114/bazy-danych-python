from pymongo import MongoClient
import pandas as pd
import cx_Oracle
import numpy as np

try:
    cx_Oracle.init_oracle_client(lib_dir="D:\\Studia\\mag_sem1\\bazy_danych\\laby\\instantclient_21_9")
    mongo = MongoClient("localhost", 50001)
    dsn_tns = cx_Oracle.makedsn('217.173.198.136', '1521', service_name='whdb')
    con = cx_Oracle.connect(user='s98784', password='s98784', dsn=dsn_tns)
    print(f"Connection provided. Connection version: {con.version}")
    print(mongo.list_database_names(), "\n")

except cx_Oracle.DatabaseError as e:
    print("There is a problem with Oracle: ", e)
    mongo.close()
    if con:
        con.close()

cx_lekarz = pd.read_sql("select * from Lekarz", con=con)
cx_urlop = pd.read_sql("select * from Urlop", con=con)
cx_wizyta = pd.read_sql("select * from Wizyta", con=con)
cx_pacjent = pd.read_sql("select * from Pacjent", con=con)
cx_chirurgia = pd.read_sql("select * from Chirurgia", con=con)
cx_diagnostyka = pd.read_sql("select * from Diagnostyka", con=con)

db = mongo["weterynaria"]
lekarz = db["lekarz"]
urlop = db["urlop"]
wizyta = db["wizyta"]
pacjent = db["pacjent"]
chirurgia = db["chirurgia"]
diagnostyka = db["diagnostyka"]

# data = cx_lekarz.to_dict('records')
# ids = lekarz.insert_many(data)
# # for l in lekarz.find(): print(l)

# data = cx_urlop.to_dict('records')
# ids = urlop.insert_many(data)
# # for l in urlop.find(): print(l)

# data = cx_wizyta.to_dict('records')
# ids = wizyta.insert_many(data)
# # for l in wizyta.find(): print(l)

# data = cx_pacjent.to_dict('records')
# ids = pacjent.insert_many(data)
# # for l in pacjent.find(): print(l)

# data = cx_chirurgia.to_dict('records')
# ids = chirurgia.insert_many(data)
# # for l in chirurgia.find(): print(l)

# data = cx_diagnostyka.to_dict('records')
# ids = diagnostyka.insert_many(data)
# for l in diagnostyka.find(): print(l)

# print(db.list_collection_names())
# print(lekarz.find_one(filter={"NAZWISKO": "Garlett"}))

# lek = lekarz.find_one(filter={"NAZWISKO": "Garlett"})
# print(f'\nImie: {lek["IMIE"]}\nNazwisko: {lek["NAZWISKO"]}\nWypłata: {lek["WYPLATA"]}')

# lek = lekarz.find_one(filter={"ID": 15})
# print(f'\nImie: {lek["IMIE"]}\nNazwisko: {lek["NAZWISKO"]}\nWypłata: {lek["WYPLATA"]}')

# pac = pacjent.find_one(filter={"GATUNEK_ZWIERZAKA": "Red deer"})
# print(f'\nImie: {pac["IMIE"]}\nNazwisko: {pac["NAZWISKO"]}\nGatunek zwierzaka: {pac["GATUNEK_ZWIERZAKA"]}')

#lek = lekarz.find(filter={"WYPLATA": {"$gt": 5000, "$lt": 6000}})
#for i in lek:
#    print(f'\nImie: {i["IMIE"]}\nNazwisko: {i["NAZWISKO"]}\nWypłata: {i["WYPLATA"]}')

# pac = pacjent.find(filter={"IMIE": "Thayne"})
# for i in pac:
#     print(f'\nImie: {i["IMIE"]}\nNazwisko: {i["NAZWISKO"]}\nAdres: {i["ADRES"]}')

# wiz = wizyta.find(filter={"CENA": {"$gt": 500, "$lt": 1000}})
# for i in wiz:
#     print(f'\nData: {i["DATA"]}\nId pacjenta: {i["PACJENT_ID"]}\nCena: {i["CENA"]}')

pipeline = [
    {
        '$lookup': {
            'from': 'wizyta',
            'localField': 'ID',
            'foreignField': 'LEKARZ_ID',
            'as': 'wizyty'
        }
    },
    {
        '$project': {
            'imie': 1,
            'nazwisko': 1,
            'liczba_wizyt': {'$size': '$wizyty'}
        }
    }
]

results = lekarz.aggregate(pipeline)

for result in results:
    print(result)

print()

pipeline = [
    {
        '$lookup': {
            'from': 'urlop',
            'localField': 'ID',
            'foreignField': 'LEKARZ_ID',
            'as': 'urlopy'
        }
    },
    {
        '$project': {
            'imie': 1,
            'nazwisko': 1,
            'liczba_urlopow': {'$size': '$urlopy'}
        }
    }
]

results = lekarz.aggregate(pipeline)

for result in results:
    print(result)

print()

pipeline = [
        {
            '$sort': {
                'WYPLATA': -1
            }
        },  
        {
            '$limit': 1
        },  
        {
            '$project': {
                'imie': 1,
                'nazwisko': 1,
                'wyplata': {'$max': '$WYPLATA'}
            }
        } 
    ]

results = lekarz.aggregate(pipeline)

for result in results:
    print(f"Największą wypłatę ma: {result}")


# collection = db['lekarz']

# # Pobieranie wszystkich rekordów z kolekcji "lekarz"
# results = collection.find()

# # Wyświetlanie rekordów
# for result in results:
#     print(result)

# database = mongo['weterynaria']

# # Pobieranie listy dostępnych kolekcji
# collection_names = database.list_collection_names()

# # Iteracja po kolekcjach i usuwanie wszystkich rekordów
# for collection_name in collection_names:
#     collection = database[collection_name]
#     collection.delete_many({})

mongo.close()
con.close()