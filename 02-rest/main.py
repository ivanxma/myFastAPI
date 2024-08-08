from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import globalvar
import oci
import mysql.connector
import pandas as pd
import time




app = FastAPI()

myconfig = globalvar.myconfig

# Functions to interact with DB

def connectMySQL(myconfig):
    cnx = mysql.connector.connect(**myconfig)
    cnx.autocommit=True
    return cnx


# Perform RAG
def query_db( mydb, mytable, myrowcount, cnx, hw):
    

    if ( hw == 'yes' ) :
      myquery = """
        select /*+ SET_VAR(use_secondary_engine=forced) */ *
        from {mydb}.{mytable} a
        LIMIT {myrowcount} 
      """.format(mydb=mydb, mytable=mytable, myrowcount=myrowcount)
    else :
      myquery =  """
        select *
        from {mydb}.{mytable} a
        LIMIT {myrowcount} 
      """.format(mydb=mydb, mytable=mytable, myrowcount=myrowcount)

    print(myquery)
    mybegin = time.perf_counter()
    df = pd.read_sql_query(myquery, cnx)
    myend = time.perf_counter()
    print(f"Execute Time : {myend - mybegin:0.6f} seconds")
    
    return df


@app.get("/", response_class=HTMLResponse)
async def root(mydb: str="employees", mytable: str="employees", myrowcount: int=10):
    cnx = connectMySQL(myconfig)
    df = query_db(mydb, mytable, myrowcount, cnx, 'no')
    cnx.close()
    return df.to_html(show_dimensions=True);
    # return df.to_json(orient="records")

@app.get("/hw", response_class=HTMLResponse)
async def root(mydb: str="employees", mytable: str="employees", myrowcount: int=10):
    cnx = connectMySQL(myconfig)
    df = query_db(mydb, mytable, myrowcount, cnx, 'yes')
    cnx.close()
    return df.to_html(show_dimensions=True);
    # return df.to_json(orient="records")
