import redis
import cx_Oracle
import pickle
import sqlalchemy

r = redis.Redis(host='localhost', port=6379, db=1)

CACHE_DURATION_SEC = 60

cx_Oracle.init_oracle_client(lib_dir="D:\\Studia\\mag_sem1\\bazy_danych\\laby\\instantclient_21_9")
engine = sqlalchemy.create_engine("oracle+cx_oracle://s98784:s98784@217.173.198.136:1521/?service_name=whdb", arraysize=1000)

conn = engine.connect()

def read_database(id):
    querr = sqlalchemy.text(f"SELECT imie, nazwisko, gatunek_zwierzaka FROM pacjent WHERE id = {id}")
    result = conn.execute(querr)
    row = result.fetchone()
    return row

def write_to_cache(id, data):
    ddata = pickle.dumps(data)
    r.set(id, ddata, ex=CACHE_DURATION_SEC)

def read_from_cache(id):
    data = r.get(id)

    if data: data = pickle.loads(data)
    return data

def get_record(id):
    info = "CACHE"
    record = read_from_cache(id)

    if not record:
        info = "SQL DATABASE"
        record = read_database(id)
        write_to_cache(id, record)
    
    return(info, record)

print(get_record(5))

def read_database2(id):
    querr = sqlalchemy.text(f"SELECT imie, nazwisko, wyplata FROM lekarz WHERE id = {id}")
    result = conn.execute(querr)
    row = result.fetchone()
    return row

def get_record2(id):
    info = "CACHE"
    record = read_from_cache(id)

    if not record:
        info = "SQL DATABASE"
        record = read_database2(id)
        write_to_cache(id, record)
    
    return(info, record)

print(get_record2(15))

def read_database3(id):
    querr = sqlalchemy.text(f"select lekarz.imie, lekarz.nazwisko from lekarz join chirurgia on lekarz.id = chirurgia.lekarz_id where chirurgia.lekarz_id = {id}")
    result = conn.execute(querr)
    row = result.fetchone()
    return row

def get_record3(id):
    info = "CACHE"
    record = read_from_cache(id)

    if not record:
        info = "SQL DATABASE"
        record = read_database2(id)
        write_to_cache(id, record)
    
    return(info, record)

print(get_record2(10))

r.flushall()
engine.dispose()
conn.close()