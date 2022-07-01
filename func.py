import io
import json
import cx_Oracle
import oci
import os
from zipfile import ZipFile
import string
import random
from timeit import default_timer as timer
import base64
from fdk import response


dbuser = 
dbpwd = 
dbsvc = "metadatadb_medium"
dbwallet_dir = os.getenv('TNS_ADMIN')

# Create the DB Session Pool

dbpool = cx_Oracle.SessionPool(dbuser, dbpwd, dbsvc, min=1, max=3, encoding="UTF-8", nencoding="UTF-8")

print("INFO: DB pool created")
#sql_statement="select sysdate from dual"

#
# Function Handler: executed every time the function is invoked
#

def handler(ctx, data: io.BytesIO = None):
    try:
        payload_bytes = data.getvalue()
        if payload_bytes==b'':
            raise KeyError('No keys in payload')
        payload = json.loads(payload_bytes)
        for row in payload:
            CarMetaData=row['value']
            CarMetaData=base64.b64decode(CarMetaData)
            CarMetaData=CarMetaData.decode('ascii')
            print(CarMetaData)
            #print(json.dumps(CarMetaData))
            sql_statement = 'insert into VW_Metadata_IOT ( ID, SPEED, METADATA) values (:ID, :SPEED, :METADATA)'
            with dbpool.acquire() as dbconnection:

                print("INFO: DB connection acquired ")

                with dbconnection.cursor() as dbcursor:

                    result = dbcursor.execute(sql_statement, [row['offset'],  row['stream'], CarMetaData])
                    dbcursor.rowfactory = lambda *args: dict(zip([d[0] for d in dbcursor.description], args))
                    result = dbconnection.commit()
                    print(result)
                    print("INFO: DB query executed ")

    except Exception as ex:
        print('ERROR: Missing key in payload', ex, flush=True)
        raise

    return response.Response(
        ctx,
        response_data=json.dumps(
            {"sql_statement": "{}".format(sql_statement), "Payload": "{}".format(payload)}),
        headers={"Content-Type": "application/json"}
    )
