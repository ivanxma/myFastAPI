from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import globalvar
import oci
import mysql.connector
import pandas as pd




app = FastAPI()

myconfig = globalvar.myconfig

# Functions to interact with DB

def connectMySQL(myconfig):
    cnx = mysql.connector.connect(**myconfig)
    return cnx


# Perform RAG
def query_db( mydb, mytable, myrowcount, cnx):

    myquery =  """
      select *
      from {mydb}.{mytable} a
      LIMIT {myrowcount} 
    """.format(mydb=mydb, mytable=mytable, myrowcount=myrowcount)
    df = pd.read_sql_query(myquery, cnx)
    return df


@app.get("/", response_class=HTMLResponse)
async def root(mydb: str="employees", mytable: str="employees", myrowcount: int=10):
    cnx = connectMySQL(myconfig)
    df = query_db(mydb, mytable, myrowcount, cnx)
    cnx.close()
    #return df.to_html(show_dimensions=True);
    return df.to_json(orient="records")
