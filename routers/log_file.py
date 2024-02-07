from typing import List, Optional
from fastapi import Depends, FastAPI, HTTPException, Request, Body, Depends, Request
import datetime
import psycopg2
import uvicorn, json
from datetime import datetime
from .database import connectToDB

def add_api_log(action_date: Optional[str] = None, payload: Optional[dict] = None, response: Optional[dict] = None,
                status: Optional[str] = None):
    try:
        engine = connectToDB()
        cur = engine.cursor()
        q_data = f"""insert into bsap_api_log (action_date,payload,response,status,create_date,write_date) values('{action_date}','{json.dumps(payload)}','{json.dumps(response)}','Success','{datetime.now()}','{datetime.now()}')"""
        api_query = cur.execute(q_data)
        engine.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        # print("Exception====", error)
        return error
