from traceback import print_tb
from typing import List, Optional
from fastapi import Depends, FastAPI, HTTPException, Request, Body, Depends, Request, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse
import requests
import http.client as client
import socket
# import pandas as pd
import datetime
import pytz, re, urllib
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import http
import random, string
import psycopg2
from .auth.jwt_handler import signJWT, decodeJWT, validate_token, return_employee_data
from .auth.jwt_bearer import jwtBearer
from .auth.send_otp import send_sms_link
from calendar import monthrange
# from .app.auth.security import validate_token
import uvicorn, json
from dateutil.relativedelta import relativedelta
from decouple import config
from psycopg2.extras import RealDictCursor
import shutil
from datetime import datetime
from datetime import timedelta
import pathlib
from .database import connectToDB
from .log_file import add_api_log
from datetime import datetime, timedelta
from dateutil.relativedelta import *


router = APIRouter()

# def date_match(date_str):
#     '''
#         function for convert datetime to psql database datetime.
#     '''
#     if date_str:
#         start_date_time_str = date_str + " " + "00:00:00.00000"
#         date_format_str = '%d-%m-%Y %H:%M:%S.%f'
#         start_given_time = datetime.strptime(start_date_time_str, date_format_str)
#         final_start_time = start_given_time - timedelta(minutes=330)
#         final_start_time_str = final_start_time.strftime('%Y-%m-%d %H:%M:%S.%f')
#         final_end_time = final_start_time + timedelta(hours=24)
#         final_end_time_str = final_end_time.strftime('%Y-%m-%d %H:%M:%S.%f')
#         return final_start_time_str, final_end_time_str


def date_match(date_str):
    '''
        function for convert datetime to psql database datetime.
    '''
    if date_str:
        start_date_time_str = date_str + " " + "00:00:00.00000"
        date_format_str = '%d-%m-%Y %H:%M:%S.%f'
        start_given_time = datetime.strptime(start_date_time_str, date_format_str)

        final_start_time = start_given_time + timedelta(minutes=330, days=1)
        final_end_time_str = final_start_time.strftime('%Y-%m-%d %H:%M:%S.%f')

        start_date = final_start_time + relativedelta(months=-1)
        final_end_time = start_date - timedelta(hours=24)
        start_end_time_str = final_end_time.strftime('%Y-%m-%d %H:%M:%S.%f')

        return start_end_time_str, final_end_time_str

#
@router.post('/applyEOrderly/')
async def ApplyEOrderly(request: dict = Body(...)):
    try:
        engine = connectToDB()
        engine.autocommit = False
        date_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        date_time = date_time.strftime("%Y-%m-%d %H:%M:%S")
        if not request.get('complainant_pranNo') or not  request.get('complainant_id') or not request.get('complaint_against_pran') or not request.get('alligation') or not request.get('date'):

            return {
                    "status": 200,
                    "message": "Please filled all mandatory fields.",
                    }
        complainant_pranNo, complainant_name, complainant_address, complainant_id, complaint_against_name, complaint_against_pran, alligation, date = request.get('complainant_pranNo'), request.get('complainant_name'), request.get('complainant_address'), request.get('complainant_id'), request.get('complaint_against_name'), request.get('complaint_against_pran'), request.get('alligation'), request.get('date')
        if engine:
            cur = engine.cursor()
            qdata = cur.execute(
                f"select id,work_phone,user_id,name,work_email from hr_employee where gpf_no = '{complaint_against_pran}';")
            rows = cur.fetchall()
            print(rows, 'ROWSSSSSS')
            if not rows:

                data = {
                    "status": 200,
                    "message": "Invalid complaint against PranNo",
                    }
                engine.commit()
                add_api_log(date_time, request, data, 'Success')
                return data
            create_user_data = cur.execute(
                f"select id,work_phone,user_id,name,work_email from hr_employee where gpf_no = '{complainant_pranNo}';")
            rows_data = cur.fetchall()
            print(rows_data, 'ROWSSSSSS')
            if not rows_data:
                engine.commit()
                data = {
                    "status": 200,
                    "message": "Invalid complainant pranNo",
                    }
                add_api_log(date_time, request, data, 'Success')
                return data

            # insert the eorderly data
            apply_date = datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d')
            eorderly_query = cur.execute(
                f"insert into bsap_vigilance (name_of_complaint,complaint_against,address,allegation_in_brief,date_of_receipt, state, create_date, write_date, create_uid, write_uid) values('{rows_data[0][0]}','{rows[0][0]}','{complainant_address}','{alligation}','{apply_date}','draft', '{datetime.now()}', '{datetime.now()}', '{rows_data[0][2]}', '{rows_data[0][2]}')")
            engine.commit()
            data = {
                    "status": 200,
                    "message": "E-orderly Applied Sucessfully",
                    }
            add_api_log(date_time, request, data, 'Success')
            engine.commit()
            return data
        else:
            data = {"status": 403, "message": "Something went wrong. Please try again later."}
            add_api_log(date_time, request, data, 'Failed')
            engine.commit()
            return data

    except (Exception, psycopg2.DatabaseError) as error:
        # print("error==EEEEEEEEEEEEEEEEEEE==", error)
        engine.rollback()
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, error, 'Failed')
        return {
            "status": 403,
            "message": str(error)
        }
    finally:
        if engine:
            cur.close()
            print("PostgreSQL connection is closed")


@router.post('/getEorderlyList/')
async def EorderlyList(request: dict = Body(...)):
    try:
        engine = connectToDB()
        engine.autocommit = False
        date_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        date_time = date_time.strftime("%Y-%m-%d %H:%M:%S")
        if not request.get('pranNo') or not request.get('date'):
            return {
                    "status": 200,
                    "message": "Please filled all mandatory fields.",
                    "data": []
                    }
        pranNo, date = request.get('pranNo'), request.get('date')
        if engine and pranNo and date:
            cur = engine.cursor()
            qdata = cur.execute(
                f"select id,user_id from hr_employee where gpf_no = '{pranNo}';")
            rows = cur.fetchall()
            if not rows:
                qdata = cur.execute(
                    f"select id,user_id from hr_employee where employee_login_id = '{pranNo}';")
                rows = cur.fetchall()
            # print(rows, 'ROWSSSSSS')
            if not rows:
                data = {
                    "status": 200,
                    "message": "Invalid User",
                    "data": []
                }
                add_api_log(date_time, request, data, 'Success')
                engine.commit()
                return data
            apply_end_date = datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d')  # this give str type end date
            apply_start_date = (datetime.strptime(date, '%d-%m-%Y') + relativedelta(months=-1)).strftime('%Y-%m-%d') # this will give a one month back date in string.
            print(apply_end_date, apply_start_date)
            eorderly_data = cur.execute(
                f"select bv.id as id, bv.vigilance_sequence as eorderly_no, he.name as complainant_name , bv.address as complainant_address, bv.allegation_in_brief as alligation, bv.state as status, bv.date_of_receipt as applied_date, bv.pending_at as action_pending_at, hh.name as complaint_against_name from bsap_vigilance bv join hr_employee he on he.id = bv.name_of_complaint join hr_employee hh on hh.id = bv.complaint_against where bv.create_uid = '{rows[0][1]}' and bv.date_of_receipt >= '{apply_start_date}' and bv.date_of_receipt <= '{apply_end_date}' order by bv.id Desc;")
            eorderly_rows = cur.fetchall()
            print(eorderly_rows, 'eorderly_rows')
            if not eorderly_rows:
                data = {
                    "status": 200,
                    "message": "No Data Found in the database",
                    "data": []
                }
                add_api_log(date_time, request, data, 'Success')
                engine.commit()
                return data


            if eorderly_rows:
                data = {
                        "status": 200,
                        "message": "Data Fetched Sucessfully",
                        "data": []
                }
                for record in eorderly_rows:
                    data_dict = {
                            "id": int(record[0]) if record[0] else '',
                            "eorderly_no": record[1] if record[1] else '',
                            "complainant_name": record[2] if record[2] else '',
                            "complainant_address": record[3] if record[3] else '',
                            "alligation": record[4] if record[4] else '',
                            "application_status": record[5] if record[5] else '',
                            "action_pending_at": record[7] if record[7] else '',
                            "applied_date": record[6].strftime("%d-%m-%Y") if record[6] else '',
                            "complaint_against_name": record[8] if record[8] else '',
                        }
                    data['data'].append(data_dict)

                add_api_log(date_time, request, data, 'Success')
                engine.commit()
                return data
            else:
                data = {"status": 403, "message": "Something went wrong. Please try again later.", "data": []}
                add_api_log(date_time, request, data, 'Failed')
                engine.commit()
                return data
        else:
            data = {"status": 403, "message": "Something went wrong. Please try again later.", "data": []}
            add_api_log(date_time, request, data, 'Failed')
            engine.commit()
            return data

    except (Exception, psycopg2.DatabaseError) as error:
        print("error==EEEEEEEEEEEEEEEEEEE==", error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, error, 'Failed')
        engine.rollback()
        return {
            "status": 403,
            "message": str(error),
            "data": []
        }
    finally:
        if engine:
            cur.close()
            engine.close()
            print("PostgreSQL connection is closed")

# , dependencies=[Depends(validate_token)]
@router.get("/getRequestType")
def getrequesttype():
    '''
        provide all the type for e-request with id and name .
    '''
    try:
        engine = connectToDB()
        engine.autocommit = False
        cur = engine.cursor()
        myquery = cur.execute(
            f"""select id, name, code from bsap_request_type;""")
        fetchmyquery = cur.fetchall()
        print(fetchmyquery, '************')
        data_list = []

        if not fetchmyquery:
            response = {
                "status": 200,
                "message": "No Record found in the system",
                "data": []
            }
        else:
            for data in fetchmyquery:
                data_dict = {
                    'id': data[0],
                    'request_type_name': data[1],
                    'code': data[2]
                }
                data_list.append(data_dict)
            response = {
                "status": 200,
                "message": "Grievance Type List",
                "data": data_list
            }
        engine.commit()
        return response
    except (Exception, psycopg2.DatabaseError) as error:
        engine.rollback()
        return {
            "status": 403,
            "message": str(error),
            "data": []
        }
    finally:
        if engine:
            cur.close()
            engine.close()
            print("PostgreSQL connection is closed")


@router.post("/applyErequest")
def applyErequest(pranNo: str = Body(..., Embed=True), request_type_id: int = Body(..., Embed=True),
                   requesting_for: str = Body(..., Embed=True),description: str = Body(..., Embed=True),
                  file: str = Body(..., Embed=True)):
    '''
        for running this api we need to run BSAP odoo because I have called a controller from bsap_greivance
        moodule for save the file in file store because on binary field attachments = True.
        for running this api we need to run BSAP odoo server everytime.
    '''
    try:
        base_url = config("base_url")
        print(base_url, "URLLLLLLLLLLL")
        engine = connectToDB()
        cur = engine.cursor()
        if not pranNo:
            response_data = {
                "status": 404,
                "pranNo": pranNo,
                "message": "Please provide PranNo",
            }
            return response_data
        if engine and pranNo:
            employee_data = cur.execute(
                f"""select id, user_id from hr_employee where gpf_no = '{pranNo}'; """)
            employee_details = cur.fetchone()
            if not employee_details:
                employee_data = cur.execute(
                    f"""select id, user_id from hr_employee where employee_login_id = '{pranNo}'; """)
                employee_details = cur.fetchone()
            print(employee_details, '***************')
            if not employee_details:
                response_data = {
                    "status": 404,
                    "message": "User not found in the Database",
                }
                return response_data

        # file_extension_list = ['.jpeg', '.png', '.pdf', '.mp3', '.mp4']
        # if file_extension not in file_extension_list:
        #     response_data = {
        #         "status": 404,
        #         "message": "Only .jpeg, .png, .pdf, .mp3, .mp4 format are allowed. Maximum file size is 10 MB",
        #     }
        #     return response_data

        # url = "http://localhost:8089/upload_images2"
        # url = "http://164.164.122.163:8087/upload_images2"
        # url = "http://164.164.122.163:8087/" + "erequest/apply"
        url = base_url + "erequest/apply"
        # url = "http://localhost:9098/erequest/apply"
        print(url, 'UUUUUUUUUUUU')

        payload = {
            "params": {
                "pranNo": pranNo,
                "request_type_id": request_type_id,
                "requesting_for": requesting_for,
                "description": description,
                "file": file,
            }
        }
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        res = json.loads(response.text)
        print('response', response.text)
        if 'record_id' in res["result"]:
            record_id = res['result']['record_id']
            cur = engine.cursor()
            upd_face_query = cur.execute(
                f"update bsap_erequest set create_uid = '{employee_details[1]}',write_uid = '{employee_details[1]}' where id = '{record_id}'")
            engine.commit()
            cur.close()

            response_data = {
                "status": 200,
                "message": "E-request Applied Sucessfully",
                "obj_id": res['result']['record_id']
            }
            return response_data
        else:
            response_data = {
                "status": 400,
                "message": res,
            }
            return response_data

    except (Exception, psycopg2.DatabaseError) as error:
        return {
            "status": 403,
            "message": str(error)
        }


@router.post("/getErequest")
def erquest_list(pranNo: str = Body(..., Embed=True), date: str = Body(..., Embed=True)):
    '''
        api for return erequest data on the basis of date and pranno.
        request_params : {"pranNo" = "1234", "date": "2022-09-02"}
    '''
    data_dict = {}
    data = []
    response_data = {}
    request_pram = {}
    request_pram['pranNo'] = pranNo
    request_pram['date'] = date

    try:
        engine = connectToDB()

        if not pranNo or date:
            d = datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d')
            response_data = {
                "status": 200,
                "pranNo": pranNo,
                "message": "Please provide PranNo and Date",
            }

        if engine and pranNo and date:
            cur = engine.cursor()
            response_data = {
                "status": 200,
                "isRa": False,
                "pranNo": pranNo,
                "message": "Successfull",
            }
            start_date, end_date = date_match(date)
            # print(start_date, end_date, 'start_date, end_date ')
            # end_date = datetime.strptime(str(date), '%d-%m-%Y').replace(tzinfo=None).strftime("%Y-%m-%d")
            # start_date = (datetime.strptime(str(date), '%d-%m-%Y') + relativedelta(months=-1)).replace(
            #     tzinfo=None).strftime('%Y-%m-%d')
            employee_data = cur.execute(f"""select id from hr_employee where gpf_no = '{pranNo}'; """)
            employee_id = cur.fetchall()
            if not employee_id:
                employee_data = cur.execute(f"""select id from hr_employee where employee_login_id = '{pranNo}'; """)
                employee_id = cur.fetchall()
            # print(employee_id, 'DEEEEEEEEEEEEEEEEEEE')
            if not employee_id:
                response_data = {
                    "status": 200,
                    "pranNo": pranNo,
                    "message": "User not found in the Database",
                    "data": []
                }
                return response_data
            # print(employee_id, 'employee_id')

            exising_request = cur.execute(
                f"""select be.id as ID, be.code as erequest_no, he.name as employee_name, be.state as application_status, brt.name as request_type_name, be.pending_at as action_pending_at,  be.applied_date as applied_date  from bsap_erequest be  join hr_employee he on he.id = be.employee_id join bsap_request_type brt on brt.id = be.erequest_type_id where be.employee_id = {employee_id[0][0]} and be.create_date >=  '{start_date}' and be.create_date <=  '{end_date}' and be.state != '{"draft"}' order by be.id DESC""")
            exising_request_data = cur.fetchall()
            print("DEEPAK", exising_request_data)
            if exising_request_data:
                erquest_status_dict = {
                    'apply': 'Applied',
                    'receive': 'Received',
                    'in_progress': 'In Progress',
                    'verify': 'Verified',
                    'approve': 'FO Pending',
                    'closed': 'Closed',
                    'reject': 'Rejected'
                }
                for row in exising_request_data:
                    key = row[3] if row[3] else ' '
                    data_dict = {
                        "id": str(row[0]) if row[0] else ' ',
                        "erequest_no": row[1] if row[1] else ' ',
                        "employee_name": row[2] if row[2] else ' ',
                        "application_status": erquest_status_dict[key],
                        "request_type_name": row[4] if row[4] else ' ',
                        "action_pending_at": row[5].replace('Pending at ', '') if row[5] else ' ',
                        "applied_date": row[6].date().strftime("%d-%m-%Y"),
                        # "created_date": (row[10] + timedelta(minutes=330)).strftime('%d-%m-%Y'),
                    }
                    data.append(data_dict)
                response_data = {
                    "status": 200,
                    "pranNo": pranNo,
                    "message": "Successfull",
                    "data": data
                }
                return response_data

            if not exising_request_data:
                response_data = {
                    "status": 200,
                    "pranNo": pranNo,
                    "message": "No Record Found in the database",
                    "data": []
                }
                return response_data

        else:
            # add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request_pram,
            #             {"status": 403, "error": "Something went wrong. Please try again later."}, 'Failed')
            return {"status": 403, "error": "Something went wrong. Please try again later."}

    except (Exception, psycopg2.DatabaseError) as error:
        print("error==attendance request appled pending requestt==", error)
        # add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request_pram, error,
        #             'Failed')
        return {
            "status": 403,
            "message": str(error)
        }