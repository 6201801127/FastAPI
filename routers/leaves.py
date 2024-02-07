from traceback import print_tb
from typing import List, Optional
from fastapi import Depends, FastAPI, HTTPException, Request, Body, Depends, Request, APIRouter
import datetime
import pytz, re, urllib
import psycopg2
from .auth.jwt_handler import signJWT, decodeJWT, validate_token, return_employee_data
from .auth.jwt_bearer import jwtBearer
from .auth.send_otp import send_sms_link
from calendar import monthrange
# from .auth.security import validate_token
import uvicorn, json
from dateutil.relativedelta import relativedelta
from decouple import config
from datetime import datetime
from .database import connectToDB
from .log_file import add_api_log
from datetime import datetime, date
from datetime import timedelta


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

@router.post('/getleaveList/', dependencies=[Depends(validate_token)])
async def get_leave_list(request: dict = Body(...)):
    try:
        engine = connectToDB()
        pranNo,date_to,isra= request.get('pranNo'),request.get('date'),"False"
        start_date_final, end_date_final = date_match(date_to)
        if engine and pranNo and date_to:
            cur = engine.cursor()
            #logic
            emp_data = cur.execute(f"select id,user_id,name,parent_id from hr_employee where gpf_no = '{pranNo}';")
            emp = cur.fetchall()
            if not emp:
                emp_data = cur.execute(f"select id,user_id,name,parent_id from hr_employee where employee_login_id = '{pranNo}';")
                emp = cur.fetchall()
            if not emp:
                response_data = {
                    "status": 200,
                    "message": "User is not found in the system",
                    "data": []
                }
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {},
                            response_data, 'Success')
                return response_data
            print("request.get('date') request.get('date') ", request.get('date'),datetime.strptime(str((request.get('date'))),'%d-%m-%Y').date())
            end_date = datetime.strptime(str(request.get('date')),'%d-%m-%Y').replace(tzinfo=None).strftime("%Y-%m-%d")
            start_date = (datetime.strptime(str(request.get('date')), '%d-%m-%Y') + relativedelta(months=-1)).replace(
                tzinfo=None).strftime('%Y-%m-%d')
            # end_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")
            # end_date = datetime.strptime(str(end_date),'%Y-%m-%d %H:%M:%S').replace(tzinfo=None).strftime("%Y-%m-%d")
            print("jjjjjjjjjjjjjjjjjjjjjjstartdate", start_date, end_date)
            leave_data = cur.execute(f"""select a.id,b.leave_type,state,a.commuted_leave_selection,a.request_date_from,a.request_date_to,a.number_of_days,a.private_name,a.manager_designation_id,a.pending_since,a.create_date, a.depart_latt, a.depart_long, a.depart_datetime, a.arr_latt, a.arr_long,a.arr_datetime from hr_leave a join hr_leave_type b on a.holiday_status_id = b.id where a.employee_id = '{emp[0][0]}' and CAST(a.create_date as Date) >= '{start_date}' and CAST(a.create_date as Date) <= '{end_date}' order by id DESC """)
            # leave_data = cur.execute(f"""select a.id,b.leave_type,state,a.commuted_leave_selection,a.request_date_from,a.request_date_to,a.number_of_days,a.private_name,a.manager_designation_id,a.pending_since,a.create_date from hr_leave a join hr_leave_type b on a.holiday_status_id = b.id where a.employee_id = '{emp[0][0]}' and ((a.request_date_from  >= '{start_date}' and a.request_date_from <='{end_date}') or (a.request_date_to  >= '{start_date}' and a.request_date_to <='{end_date}') or (a.request_date_from  <= '{start_date}' and a.request_date_to >='{start_date}') or (a.request_date_from  <= '{end_date}' and a.request_date_to >='{end_date}')); """)
            leaves = cur.fetchall()
            print("leave_data >>>>>>>>>>>>>>>>>>>>>> ", start_date, end_date, leaves)
            data = []
            status = ''
            departure = ''
            arrival = ''
            if emp[0][3]:
               isra = "True"
            print("No",emp[0][3],isra)
            if leaves:
                response_data = {
                    'status':200
                }
                for record in leaves:
                    print("=format", format(record[10]))
                    if record[2] in ('draft', 'confirm'):
                        status = 'Pending'
                        departure = False
                        arrival = False
                    if record[2] in ('cancel','refuse'):
                        status = 'Rejected'
                        departure = False
                        arrival = False
                    if record[2] == 'validate':
                        status = 'Approved'
                        departure = True
                        arrival = True
                        if record[11] == None and record[12] == None and record[13] == None:
                            departure = False
                        if record[14] == None and record[15] == None and record[16] == None:
                            arrival = False

                    request_data = {
                            "leave_id": record[0],
                            "leave_type_name": record[1],
                            "leave_status": status,
                            "commuted_type": record[3],
                            "start_date": datetime.strptime(str(record[4]),'%Y-%m-%d').strftime("%d-%m-%Y") if record[4] else "",
                            "end_date": datetime.strptime(str(record[5]),'%Y-%m-%d').strftime("%d-%m-%Y") if record[5] else "",
                            "requested_days": record[6],
                            "description": record[7] if record[7] else "",
                            "pending_since": datetime.strptime(str(record[9]),'%Y-%m-%d').strftime("%d-%m-%Y") if record[9] else "",
                            "create_date": datetime.strptime(str(record[10]),'%Y-%m-%d %H:%M:%S.%f').strftime("%d-%m-%Y") if record[10] else "",
                            "departure": departure,
                            "arrival": arrival
                            # "departure_leave_url": config("base_url") + "my/departure/leave/" + str(record[0]) + "/download",
                            # "departure_leave_url": config("base_url") + "my/leave/certificate/" + str(record[0]) + "/download",
                        }
                    data.append(request_data)
                cur.close()
                response_data['isra']= isra
                response_data['pranNo'] = pranNo
                response_data['message']= "Success"
                response_data['data']= data
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},response_data,'Success')
                return response_data
            else:
                cur.close()
                response_data = {
                    'status':200,
                }
                response_data['isra']= isra
                response_data['message']= "No pending request found."
                response_data['data']= data
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},response_data,'Success')
                return response_data
        else:
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},{"status": 403,"message":"Something went wrong. Please try again later."},'Failed')
            return {"status": 403,"message":"Something went wrong. Please try again later."}
    except (Exception, psycopg2.DatabaseError) as error:
        print("error==attendance pending requestt==",error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},error,'Failed')
        return error

@router.post('/getallleaveList/', dependencies=[Depends(validate_token)])
async def get_allleave_list(request: dict = Body(...)):
    try:
        engine = connectToDB()

        pranNo,date_to= request.get('pranNo'),request.get('date')
        if not request.get('date'):
            response_data = {
                    "status": 200,
                    "message": "Date is not provided",
                }
            return response_data
        if engine and pranNo and date_to:
            cur = engine.cursor()
            #logic
            emp_data = cur.execute(f"select id,user_id,name,parent_id from hr_employee where gpf_no = '{pranNo}';")
            emp = cur.fetchall()
            if not emp:
                emp_data = cur.execute(f"select id,user_id,name,parent_id from hr_employee where employee_login_id = '{pranNo}';")
                emp = cur.fetchall()
            if not emp:
                response_data = {
                    "status": 200,
                    "message": "User is not found in the system",
                    "pending_leaves": [],
                    "approved_leaves": [],
                    "rejected_leaves": []
                }
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {},
                            response_data, 'Success')
                return response_data
            if emp:
                end_date = datetime.strptime(str(request.get('date')), '%d-%m-%Y').replace(tzinfo=None).strftime(
                    "%Y-%m-%d")
                start_date = (datetime.strptime(str(request.get('date')), '%d-%m-%Y') + relativedelta(months=-1)).replace(
                    tzinfo=None).strftime('%Y-%m-%d')

                # end_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")
                # end_date = datetime.strptime(str(end_date), '%Y-%m-%d %H:%M:%S').replace(tzinfo=None).strftime(
                #     "%Y-%m-%d")
                print(f"start_date >>>> {start_date} ||| end_date >>>> {end_date}")


                leave_qry = f"""select a.id,b.leave_type,a.state,a.commuted_leave_selection,a.request_date_from,a.request_date_to,a.number_of_days,a.private_name,a.manager_designation_id,a.pending_since,a.create_date,a.cancel_req, hr.name, hr.gpf_no from hr_leave a join hr_employee hr on hr.id = a.employee_id join hr_leave_type b on a.holiday_status_id = b.id join hr_employee he on he.id = a.employee_id  where he.parent_id = '{emp[0][0]}' and  CAST(a.create_date as Date)  >= '{start_date}' and CAST(a.create_date as Date)  <= '{end_date}'"""

                print(leave_qry + " and a.state in ('validate')", 'QUERYYYYYYYY')
                leave_data_confirm = cur.execute(leave_qry + " and a.state in ('draft', 'confirm') order by id DESC")
                leaves_pending = cur.fetchall()

                leave_data_approved = cur.execute(leave_qry + " and a.state in ('validate') order by id DESC")
                leaves_approved = cur.fetchall()
                print(leaves_approved, 'APPPPPPPPPPPPPP')

                leave_data_rejected = cur.execute(leave_qry + " and a.state in ('refuse','cancel') order by id DESC")
                leaves_rejected = cur.fetchall()

                print("leave_data >>>>>>>>>>>>> ", leaves_pending, leaves_approved, leaves_rejected)
                pending_leaves = []
                approved_leaves = []
                rejected_leaves = []
                status = ''
                if leaves_pending or leaves_approved or leaves_rejected:
                    response_data = {
                        'status': 200
                    }
                    for record in leaves_pending:

                        if record[2] in ('draft', 'confirm'):
                            status = 'Pending'
                        request_data = {
                            "PranNo": record[13],
                            "employee_name": record[12],
                            "leave_id": record[0],
                            "leave_type_name": record[1],
                            "leave_status": status,
                            "commuted_type": record[3],
                            "start_date": datetime.strptime(str(record[4]), '%Y-%m-%d').strftime("%d-%m-%Y") if
                            record[4] else "",
                            "end_date": datetime.strptime(str(record[5]), '%Y-%m-%d').strftime("%d-%m-%Y") if
                            record[5] else "",
                            "requested_days": record[6],
                            "description": record[7] if record[7] else "",
                            # "pending_with": record[8],
                            "pending_since": datetime.strptime(str(record[9]), '%Y-%m-%d').strftime("%d-%m-%Y") if
                            record[9] else "",
                            "create_date": datetime.strptime(str(record[10]), '%Y-%m-%d %H:%M:%S.%f').strftime(
                                "%d-%m-%Y") if record[10] else "",
                            "isCancelable": False
                        }
                        pending_leaves.append(request_data)
                    print("data1", pending_leaves)

                    for record in leaves_approved:
                        if record[2] == 'validate':
                            status = 'Approved'
                        request_data = {
                            "PranNo": record[13],
                            "employee_name": record[12],
                            "leave_id": record[0],
                            "leave_type_name": record[1],
                            "leave_status": status,
                            "commuted_type": record[3],
                            "start_date": datetime.strptime(str(record[4]), '%Y-%m-%d').strftime("%d-%m-%Y") if
                            record[4] else "",
                            "end_date": datetime.strptime(str(record[5]), '%Y-%m-%d').strftime("%d-%m-%Y") if
                            record[5] else "",
                            "requested_days": record[6],
                            "description": record[7] if record[7] else "",
                            # "pending_with": record[8],
                            "pending_since": datetime.strptime(str(record[9]), '%Y-%m-%d').strftime("%d-%m-%Y") if
                            record[9] else "",
                            "create_date": datetime.strptime(str(record[10]), '%Y-%m-%d %H:%M:%S.%f').strftime(
                                "%d-%m-%Y") if record[10] else "",
                            "isCancelable": record[11]
                        }
                        approved_leaves.append(request_data)
                    print("data2", approved_leaves)

                    for record in leaves_rejected:
                        if record[2] in ('cancel', 'refuse'):
                            status = 'Rejected'
                        request_data = {
                            "PranNo": record[13],
                            "employee_name": record[12],
                            "leave_id": record[0],
                            "leave_type_name": record[1],
                            "leave_status": record[2],
                            "commuted_type": record[3],
                            "start_date": datetime.strptime(str(record[4]), '%Y-%m-%d').strftime("%d-%m-%Y") if
                            record[4] else "",
                            "end_date": datetime.strptime(str(record[5]), '%Y-%m-%d').strftime("%d-%m-%Y") if
                            record[5] else "",
                            "requested_days": record[6],
                            "description": record[7] if record[7] else "",
                            # "pending_with": record[8],
                            "pending_since": datetime.strptime(str(record[9]), '%Y-%m-%d').strftime("%d-%m-%Y") if
                            record[9] else "",
                            "create_date": datetime.strptime(str(record[10]), '%Y-%m-%d %H:%M:%S.%f').strftime(
                                "%d-%m-%Y") if record[10] else "",
                            "isCancelable": False
                        }
                        rejected_leaves.append(request_data)
                    print("data3", rejected_leaves)

                    cur.close()
                    response_data['pranNo'] = pranNo
                    response_data['message'] = "Success"
                    response_data['pending_leaves'] = pending_leaves
                    response_data['approved_leaves'] = approved_leaves
                    response_data['rejected_leaves'] = rejected_leaves
                    add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {},
                                response_data, 'Success')
                    return response_data
                else:
                    cur.close()
                    response_data = {
                        'status': 200
                    }
                    response_data['message'] = "No pending request found."
                    response_data['approved_leaves'] = []
                    response_data['rejected_leaves'] = []
                    response_data['pending_leaves'] = []

                    add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {},
                                response_data, 'Success')
                    return response_data
            else:
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {},
                            {"status": 403, "message": "Something went wrong. Please try again later."}, 'Failed')
                return {"status": 403, "message": "Something went wrong. Please try again later."}

    except (Exception, psycopg2.DatabaseError) as error:
        print("error==attendance pending requestt==",error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},error,'Failed')
        return error


# @app.post('/getallleaveListold/', dependencies=[Depends(validate_token)])
# async def get_allleave_list(request: dict = Body(...)):
#     try:
#         pranNo,date_to= request.get('pranNo'),request.get('date')
#         if engine and pranNo and date_to:
#             cur = engine.cursor()
#             #logic
#             emp_data = cur.execute(f"select id,user_id,name,parent_id from hr_employee where gpf_no = '{pranNo}';")
#             emp = cur.fetchall()
#             if not emp:
#                 response_data = {
#                     "status": 200,
#                     "message": "PranNo is not found in the system",
#                     "data": []
#                 }
#                 add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {},
#                             response_data, 'Success')
#                 return response_data
#
#             # print("request.get('date') request.get('date') ", request.get('date'),datetime.strptime(str((request.get('date'))),'%d-%m-%Y').date())
#             start_date = datetime.strptime(str(request.get('date')),'%d-%m-%Y').replace(tzinfo=None).strftime("%Y-%m-%d")
#             end_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")
#             end_date = datetime.strptime(str(end_date),'%Y-%m-%d %H:%M:%S').replace(tzinfo=None).strftime("%Y-%m-%d")
#             print(f"start_date >>>> {start_date} ||| end_date >>>> {end_date}")
#
#             leave_qry = f"""select a.id,b.leave_type,state,a.commuted_leave_selection,a.request_date_from,a.request_date_to,a.number_of_days,a.private_name,a.manager_designation_id,a.pending_since,a.create_date,a.cancel_req from hr_leave a join hr_leave_type b on a.holiday_status_id = b.id where a.employee_id = '{emp[0][0]}' and ((a.request_date_from  >= '{start_date}' and a.request_date_from <='{end_date}') or (a.request_date_to  >= '{start_date}' and a.request_date_to <='{end_date}') or (a.request_date_from  <= '{start_date}' and a.request_date_to >='{start_date}') or (a.request_date_from  <= '{end_date}' and a.request_date_to >='{end_date}'))"""
#
#             leave_data_confirm = cur.execute(leave_qry + " and a.state in ('draft', 'confirm');")
#             leaves_pending = cur.fetchall()
#
#             leave_data_approved = cur.execute(leave_qry + " and a.state in ('validate');")
#             leaves_approved = cur.fetchall()
#
#             leave_data_rejected = cur.execute(leave_qry + " and a.state in ('refuse','cancel');")
#             leaves_rejected = cur.fetchall()
#
#             print("leave_data >>>>>>>>>>>>> ", leaves_pending,leaves_approved,leaves_rejected)
#             pending_leaves = []
#             approved_leaves = []
#             rejected_leaves = []
#             status = ''
#             if leaves_pending or leaves_approved or leaves_rejected:
#                 response_data = {
#                 'status':200
#                 }
#                 for record in leaves_pending:
#
#                     if record[2] in ('draft', 'confirm'):
#                         status = 'Pending'
#                     request_data = {
#
#                             "leave_id": record[0],
#                             "leave_type_name": record[1],
#                             "leave_status": status,
#                             "commuted_type": record[3],
#                             "start_date":  datetime.strptime(str(record[4]),'%Y-%m-%d').strftime("%d-%m-%Y") if record[4] else "",
#                             "end_date":  datetime.strptime(str(record[5]),'%Y-%m-%d').strftime("%d-%m-%Y") if record[5] else "",
#                             "requested_days": record[6],
#                             "description": record[7] if record[7] else "",
#                             # "pending_with": record[8],
#                             "pending_since": datetime.strptime(str(record[9]),'%Y-%m-%d').strftime("%d-%m-%Y") if record[9] else "",
#                             "create_date": datetime.strptime(str(record[10]),'%Y-%m-%d %H:%M:%S.%f').strftime("%d-%m-%Y") if record[10] else "",
#                             "isCancelable": False
#                         }
#                     pending_leaves.append(request_data)
#                 print("data1", pending_leaves)
#
#                 for record in leaves_approved:
#                     if record[2] == 'validate':
#                         status = 'Approved'
#                     request_data = {
#                             "leave_id": record[0],
#                             "leave_type_name": record[1],
#                             "leave_status": status,
#                             "commuted_type": record[3],
#                             "start_date":  datetime.strptime(str(record[4]),'%Y-%m-%d').strftime("%d-%m-%Y") if record[4] else "",
#                             "end_date":  datetime.strptime(str(record[5]),'%Y-%m-%d').strftime("%d-%m-%Y") if record[5] else "",
#                             "requested_days": record[6],
#                             "description": record[7] if record[7] else "",
#                             # "pending_with": record[8],
#                             "pending_since": datetime.strptime(str(record[9]),'%Y-%m-%d').strftime("%d-%m-%Y") if record[9] else "",
#                             "create_date": datetime.strptime(str(record[10]),'%Y-%m-%d %H:%M:%S.%f').strftime("%d-%m-%Y") if record[10] else "",
#                             "isCancelable": record[11]
#                         }
#                     approved_leaves.append(request_data)
#                 print("data2", approved_leaves)
#
#                 for record in leaves_rejected:
#                     if record[2] in ('cancel','refuse'):
#                         status = 'Rejected'
#                     request_data = {
#                             "leave_id": record[0],
#                             "leave_type_name": record[1],
#                             "leave_status": record[2],
#                             "commuted_type": record[3],
#                             "start_date":  datetime.strptime(str(record[4]),'%Y-%m-%d').strftime("%d-%m-%Y") if record[4] else "",
#                             "end_date":  datetime.strptime(str(record[5]),'%Y-%m-%d').strftime("%d-%m-%Y") if record[5] else "",
#                             "requested_days": record[6],
#                             "description": record[7] if record[7] else "",
#                             # "pending_with": record[8],
#                             "pending_since": datetime.strptime(str(record[9]),'%Y-%m-%d').strftime("%d-%m-%Y") if record[9] else "",
#                             "create_date": datetime.strptime(str(record[10]),'%Y-%m-%d %H:%M:%S.%f').strftime("%d-%m-%Y") if record[10] else "",
#                             "isCancelable": False
#                         }
#                     rejected_leaves.append(request_data)
#                 print("data3", rejected_leaves)
#
#                 cur.close()
#                 response_data['pranNo'] = pranNo
#                 response_data['message']= "Success"
#                 response_data['pending_leaves']= pending_leaves
#                 response_data['approved_leaves']= approved_leaves
#                 response_data['rejected_leaves']= rejected_leaves
#                 add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},response_data,'Success')
#                 return response_data
#             else:
#                 cur.close()
#                 response_data = {
#                 'status':200
#                 }
#                 response_data['message']= "No pending request found."
#                 response_data['data']= []
#                 add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},response_data,'Success')
#                 return response_data
#         else:
#             add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},{"status": 403,"message":"Something went wrong. Please try again later."},'Failed')
#             return {"status": 403,"message":"Something went wrong. Please try again later."}
#     except (Exception, psycopg2.DatabaseError) as error:
#         print("error==attendance pending requestt==",error)
#         add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},error,'Failed')
#         return error

@router.post('/applyLeave/',dependencies=[Depends(validate_token)])
async def apply_leave(request: dict = Body(...)):
    try:
        """ UTC Conversion """
        engine = connectToDB()
        print('request', request)
        leave_type_id,start_date,end_date,duration,fo_applicable,commuted_type,purpose,address_during_leave,pranNo = request.get('leave_type_id'),datetime.strptime(str(request.get('start_date')),'%d-%m-%Y').astimezone(pytz.timezone('Asia/Calcutta')).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f"),datetime.strptime(str(request.get('end_date')),'%d-%m-%Y').astimezone(pytz.timezone('Asia/Calcutta')).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f"),request.get('duration'),request.get('fo_applicable'),request.get('commuted_type'),request.get('purpose'),request.get('address_during_leave'),request.get('pranNo')
        # temp = reason.replace("'", r"")
        if engine and start_date and end_date and duration and address_during_leave and pranNo:
            cur = engine.cursor()
            print("cur", cur)
            #logic
            """ 1) If no request for the given date ,insert a new request
                2) if request is present for the current date, update the existing request
            """
            emp_data = cur.execute(f"select id,user_id,current_office_id from hr_employee where gpf_no = '{pranNo}';")
            emp = cur.fetchall()
            if not emp:
                emp_data = cur.execute(
                    f"select id,user_id,current_office_id from hr_employee where employee_login_id = '{pranNo}';")
                emp = cur.fetchall()
            data = {
                    "status": 200,
                    "message": "leave Request Submitted"
                    }
            exising_approved_request = cur.execute(
                f"""select id,request_date_from,request_date_to from hr_leave where employee_id = {emp[0][0]} and (request_date_from >= '{str(start_date)}' and request_date_from <= '{str(end_date)}' and request_date_to >= '{str(end_date)}')  or (request_date_from <= '{str(start_date)}' and request_date_to >= '{str(start_date)}' and request_date_from <= '{str(end_date)}' and request_date_to >= '{str(end_date)}') or (request_date_from <= '{str(start_date)}' and request_date_to >= '{str(start_date)}' and request_date_to <= '{str(end_date)}' or (request_date_from >= '{str(start_date)}' and request_date_to <= '{str(end_date)}')) and state in ('validate', 'validate1'); """)
            exising_approved_request = cur.fetchall()
            print("exising_approved_request", exising_approved_request)
            if exising_approved_request:
                return {
                    "status": 200,
                    "message": "Leave is already approved between the provide date"
                }

            exising_request = cur.execute(f"""select id,request_date_from,request_date_to from hr_leave where employee_id = {emp[0][0]} and (request_date_from <= '{str(start_date)}' and request_date_to >= '{str(start_date)}') or (request_date_to >='{str(start_date)}' and (request_date_to >= '{str(end_date)}' or request_date_to <= '{str(end_date)}')) and state not in ('cancel','refuse', 'validate', 'validate1'); """)
            print("exising_request", exising_request)
            exising_request_data = cur.fetchall()
            print("exising_requestdata", exising_request_data)
            if exising_request_data:
                upd_face_query = cur.execute(f"update hr_leave set holiday_status_id = '{leave_type_id}',request_date_from = '{start_date}', request_date_to = '{end_date}',date_from='{start_date}', date_to='{end_date}',number_of_days = '{duration}',no_of_days_display_half = '{duration}',fo_req = '{fo_applicable}',commuted_leave_selection = '{commuted_type}', private_name = '{str(purpose)}', address_during_leave = '{str(address_during_leave)}'  where id = '{exising_request_data[0][0]}' and employee_id = '{emp[0][0]}' and  state not in ('cancel','refuse')")
                engine.commit()
                cur.close()
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),request,data,'Success')
                return data
            else:
                add_request = cur.execute(f"""insert into hr_leave (employee_id,state,holiday_status_id,holiday_type,request_date_from,request_date_to,date_from,date_to,number_of_days,no_of_days_display_half,fo_req,commuted_leave_selection,private_name,address_during_leave,create_uid,write_uid,user_id,create_date,write_date) values('{emp[0][0]}','confirm','{leave_type_id}','employee','{start_date}','{end_date}','{start_date}','{end_date}','{duration}','{duration}','{fo_applicable}','{commuted_type}','{str(purpose)}','{str(address_during_leave)}','{emp[0][1]}','{emp[0][1]}','{emp[0][1]}','{datetime.now()}','{datetime.now()}')""")
                print("add_request=",add_request)
                engine.commit()
                cur.close()
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),request,data,'Success')
                return data
        else:
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),request,{"status": 403,"message":"Something went wrong. Please try again later."},'Failed')
            return {"status": 403,"message":"Something went wrong. Please try again later."}
    except (Exception, psycopg2.DatabaseError) as error:
        print("error==leave apply requestt==",error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),request,error,'Failed')
        return error

@router.post('/getAvailableLeaves/')
async def get_available_leaves(request: dict = Body(...)):
    try:
        engine = connectToDB()
        pranNo,leave_status_id = request.get('pranNo'),request.get('leave_status_id')
        if engine and pranNo:
            cur = engine.cursor()
            #logic
            response_data = {
                'status':200
            }
            emp_data = cur.execute(f"select id,user_id,name,parent_id from hr_employee where gpf_no = '{pranNo}';")
            emp = cur.fetchall()
            if not emp:
                emp_data = cur.execute(f"select id,user_id,name,parent_id from hr_employee where employee_login_id = '{pranNo}';")
                emp = cur.fetchall()
            leave_type_data = cur.execute(f"""select id,name from hr_leave_type where name = '{str(leave_status_id)}'; """)
            leaves = cur.fetchall()
            print("leave_type", leaves)
            leave_data = cur.execute(f"""select id,employee_id,number_of_days from hr_leave where holiday_status_id = '{leaves[0][0]}' and employee_id='{emp[0][0]}' and state in ('validate1', 'validate'); """)
            leaves_request = cur.fetchall()
            print("leave_type", leaves_request, len(leaves_request))
            leave_data_applied = cur.execute(f"""select id,employee_id,number_of_days from hr_leave where holiday_status_id = '{leaves[0][0]}' and employee_id='{emp[0][0]}' and state in ('confirm', 'validate'); """)
            leaves_applied = cur.fetchall()
            print("leave_type", leaves_applied, len(leaves_applied))
            leave_allocation = cur.execute(f"""select id,employee_id,number_of_days from hr_leave_allocation where holiday_status_id = '{leaves[0][0]}' and employee_id='{emp[0][0]}' and state in ('confirm', 'validate1', 'validate'); """)
            leaves_allocation = cur.fetchall()
            print("leaves_allocation", leaves_allocation, len(leaves_allocation), type(leaves_allocation[0][2]), len(leaves_request))
            applied_bal = 0
            granted_bal = 0
            balance_bal = 0
            for record in leaves_applied:
                print('record', record, record[2])
                applied_bal += record[2]
            print('applied_bal', applied_bal)
            for record in leaves_request:
                print('record', record)
                granted_bal += record[2]
            print('granted_bal', applied_bal)
            if leaves_allocation:
                balance_bal = leaves_allocation[0][2] - granted_bal
                print("balancebalance", balance_bal)
            data = []
            print("No",emp[0][0])
            if leaves and leaves_allocation:
                response_data = {
                    'status':200
                }
                request_data = {

                        "leave_type_name": leaves[0][1],
                        "leave_type_id": leaves[0][0],
                        "entitle": leaves_allocation[0][2],
                        "applied":applied_bal,
                        "granted":granted_bal,
                        "balance":balance_bal
                    }
                data.append(request_data)
                cur.close()
                response_data['pranNo'] = pranNo
                response_data['message']= "Success"
                response_data['data']= data
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},response_data,'Success')
                return response_data
            else:
                cur.close()
                response_data = {
                    'status':200
                }
                response_data['message']= "No Data found."
                response_data['data']= data
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},response_data,'Success')
                return response_data
        else:
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},{"status": 403,"message":"Something went wrong. Please try again later."},'Failed')
            return {"status": 403,"message":"Something went wrong. Please try again later."}
    except (Exception, psycopg2.DatabaseError) as error:
        print("error==attendance pending requestt==",error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},error,'Failed')
        return error

@router.post('/leaveAction/',dependencies=[Depends(validate_token)])
async def leave_action(request: dict = Body(...)):
    try:
        """ UTC Conversion """
        print('request', request)
        engine = connectToDB()
        pranNo,leave_id,remark,leave_action = request.get('pranNo'),request.get('leave_id'),request.get('remark'),request.get('leave_action')
        # temp = reason.replace("'", r"")
        state = False
        if engine and pranNo and leave_id and remark and leave_action:
            cur = engine.cursor()
            print("cur", cur)
            #logic
            """ 1) If no request for the given date ,insert a new request
                2) if request is present for the current date, update the existing request
            """
            emp_data = cur.execute(f"select id,user_id,current_office_id from hr_employee where gpf_no = '{pranNo}';")
            emp = cur.fetchall()
            if not emp:
                emp_data = cur.execute(
                    f"select id,user_id,current_office_id from hr_employee where employee_login_id = '{pranNo}';")
                emp = cur.fetchall()
            exising_request = cur.execute(f"""select id from hr_leave where id='{leave_id}' and state in ('confirm'); """)
            exising_request_data = cur.fetchall()
            if exising_request_data:
                print("exising_request", exising_request,request.get('leave_action'),type(leave_action))
                if request.get('leave_action') == '1':
                    data = {
                            "status": 200,
                            "message": "Leave approved successfully"
                            }
                    state='validate'
                    # upd_face_query = cur.execute(f"update hr_leave set state = '{state}' where id = '{exising_request_data[0][0]}' and employee_id = '{emp[0][0]}'")
                    upd_face_query = cur.execute(f"update hr_leave set state = '{state}' where id = '{exising_request_data[0][0]}'")
                    engine.commit()
                    cur.close()
                    add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),request,data,'Success')
                    return data
                else:
                    data = {
                            "status": 200,
                            "message": "Leave Rejected successfully"
                            }
                    state='refuse'
                    upd_face_query = cur.execute(f"update hr_leave set state = '{state}' where id = '{exising_request_data[0][0]}'")
                    engine.commit()
                    cur.close()
                    add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),request,data,'Success')
                    return data
            else:
                cur.close()
                response_data = {
                'status':200
                }
                response_data['message']= "No pending request found."
                response_data['data']= []
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},response_data,'Success')
                return response_data
        else:
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),request,{"status": 403,"error":"Something went wrong. Please try again later."},'Failed')
            return {"status": 403,"message":"Something went wrong. Please try again later."}
    except (Exception, psycopg2.DatabaseError) as error:
        print("error==leave apply requestt==",error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),request,error,'Failed')
        return error

@router.get("/getallleavetype")
async def getalll_eavetype():
    try:
        variable = True
        engine = connectToDB()
        if engine:
            cur = engine.cursor()
            leaves_data = cur.execute(f"select id,leave_type from hr_leave_type where active='{variable}';")
            leaves = cur.fetchall()
            response_data = {
                    "status": 200,
                    "message": "Leave Type List"
                    }
            data = []
            if leaves:
                for record in leaves:
                    print("No",record[0],record[1])
                    request_data = {
                            "leave_id": record[0],
                            "leave_type_name": record[1],
                        }
                    data.append(request_data)
                cur.close()
                response_data['data']= data
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},response_data,'Success')
                return response_data
            else:
                cur.close()
                response_data['message']= "No Leave Type found."
                response_data['data']= []
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},response_data,'Success')
                return response_data
        else:
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},{"status": 403,"message":"Something went wrong. Please try again later."},'Failed')
            return {"status": 403,"message":"Something went wrong. Please try again later."}
    except (Exception, psycopg2.DatabaseError) as error:
        print("error==attendance pending requestt==",error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},error,'Failed')
        return error

@router.post('/leaveActioncancel/',dependencies=[Depends(validate_token)])
async def leave_action(request: dict = Body(...)):
    try:
        """ UTC Conversion """
        engine = connectToDB()
        print('request', request)
        pranNo,leave_id = request.get('pranNo'),request.get('leave_id')
        # temp = reason.replace("'", r"")
        state = False
        if engine and pranNo and leave_id:
            cur = engine.cursor()
            print("cur", cur)
            #logic
            """ 1) If no request for the given date ,insert a new request
                2) if request is present for the current date, update the existing request
            """
            emp_data = cur.execute(f"select id,user_id,current_office_id from hr_employee where gpf_no = '{pranNo}';")
            emp = cur.fetchall()
            if not emp:
                emp_data = cur.execute(
                    f"select id,user_id,current_office_id from hr_employee where employee_login_id = '{pranNo}';")
                emp = cur.fetchall()
            data = {
                    "status": 200,
                    "message": "Leave Cancelled successfully"
                    }
            exising_request = cur.execute(f"""select id from hr_leave where id='{leave_id}' and state in ('confirm'); """)
            exising_request_data = cur.fetchall()
            if exising_request_data:
                print("exising_request", exising_request,request.get('leave_action'),type(leave_action))
                state='cancel'
                iscancel = False
                upd_face_query = cur.execute(f"update hr_leave set state = '{state}', cancel_req='{iscancel}' where id = '{exising_request_data[0][0]}'")
                engine.commit()
                cur.close()
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),request,data,'Success')
                return data
            else:
                cur.close()
                data['message']= "No Leave found."
                data['data']= []
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},data,'Success')
                return data
        else:
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),request,{"status": 403,"message":"Something went wrong. Please try again later."},'Failed')
            return {"status": 403,"message":"Something went wrong. Please try again later."}
    except (Exception, psycopg2.DatabaseError) as error:
        print("error==leave apply requestt==",error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),request,error,'Failed')
        return error

@router.post('/leavecancelrequest/') #,dependencies=[Depends(validate_token)]
async def leave_cancel_request(request: dict = Body(...)):
    try:
        """ UTC Conversion """
        print('request', request)

        today_date = date.today()
        engine = connectToDB()
        pranNo,leave_id,remark = request.get('pranNo'),request.get('leave_id'), request.get('remark')
        # temp = reason.replace("'", r"")
        state = False
        if engine and pranNo and leave_id:
            cur = engine.cursor()
            print("cur", cur)
            #logic
            """ 1) If no request for the given date ,insert a new request
                2) if request is present for the current date, update the existing request
            """
            emp_data = cur.execute(f"select id,user_id,current_office_id from hr_employee where gpf_no = '{pranNo}';")
            emp = cur.fetchall()
            if not emp:
                emp_data = cur.execute(
                    f"select id,user_id,current_office_id from hr_employee where employee_login_id = '{pranNo}';")
                emp = cur.fetchall()
            data = {
                    "status": 200,
                    "message": "Leave Cancel Request Sent Successful"
                    }
            exising_request = cur.execute(f"""select id, request_date_from from hr_leave where employee_id = '{emp[0][0]}' and id='{leave_id}' and state in ('validate', 'confirm'); """)
            exising_request_data = cur.fetchall()
            if exising_request_data:
                if today_date >= exising_request_data[0][1]:
                    cur.close()
                    data['message'] = "You cannot applied cancel request for this leave"
                    data['data'] = []
                    add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {}, data,
                                'Success')
                    return data
                print("exising_request", exising_request)
                iscancel=True
                upd_face_query = cur.execute(f"update hr_leave set cancel_req = '{iscancel}', cancel_reason='{str(remark)}' where id = '{exising_request_data[0][0]}' and employee_id = '{emp[0][0]}'")
                engine.commit()
                cur.close()
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),request,data,'Success')
                return data
            else:
                cur.close()
                data['message']= "No Leave found."
                data['data']= []
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),{},data,'Success')
                return data
        else:
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),request,{"status": 403,"message":"Something went wrong. Please try again later."},'Failed')
            return {"status": 403,"message":"Something went wrong. Please try again later."}
    except (Exception, psycopg2.DatabaseError) as error:
        print("error==leave apply requestt==",error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),request,error,'Failed')
        return error


@router.post("/leave-departure")
def leave_arrival(leave_id: int = Body(..., Embed=True), departure_lat: str = Body(..., Embed=True), departure_long: str = Body(..., Embed=True),
                   date_time: str = Body(..., Embed=True),):

    try:
        if not leave_id or not departure_lat or not departure_long or not date_time:
            return {
                "status": 200,
                "Message": "please Provide all values in request params !!!"
            }
        if date_time:
            date_time = datetime.strptime(str(date_time), '%d-%m-%Y %H:%M:%S').astimezone(
                pytz.timezone('UTC')).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
        engine = connectToDB()
        cur = engine.cursor()
        leave_data = cur.execute(
            f"""select id from hr_leave where id = {leave_id}; """)
        leave_data = cur.fetchone()
        if not leave_data:
            return {
                "status": 200,
                "message": "Leave is not present in the system. Please Check leave_id in request param."
            }
        myupdatequery = f"""Update hr_leave set depart_latt = '{departure_lat}', depart_long = '{departure_long}', depart_datetime = '{date_time}' where id = {leave_id}"""
        cur.execute(myupdatequery)
        engine.commit()
        return {
            "status": 200,
            "Message": 'Leave Departure Done Successfully.',
        }

    except (Exception, psycopg2.DatabaseError) as error:
        print("error==leave apply requestt==", error)
        # add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, error, 'Failed')
        return error


@router.post("/leave-arrival")
def leave_arrival(leave_id: int = Body(..., Embed=True), arrival_lat: str = Body(..., Embed=True),
                  arrival_long: str = Body(..., Embed=True),
                  date_time: str = Body(..., Embed=True), ):
    try:
        if not leave_id or not arrival_long or not arrival_lat or not date_time:
            return {
                "status": 200,
                "Message": "please Provide all values in request params !!!"
            }
        if date_time:
            date_time = datetime.strptime(str(date_time), '%d-%m-%Y %H:%M:%S').astimezone(
                pytz.timezone('UTC')).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
        engine = connectToDB()
        cur = engine.cursor()
        leave_data = cur.execute(
            f"""select id from hr_leave where id = {leave_id}; """)
        leave_data = cur.fetchone()
        if not leave_data:
            return {
                "status": 200,
                "message": "Leave is not present in the system. Please Check leave_id in request param."
            }
        myupdatequery = f"""Update hr_leave set arr_latt = '{arrival_lat}', arr_long = '{arrival_long}', arr_datetime = '{date_time}' where id = {leave_id}"""
        cur.execute(myupdatequery)
        engine.commit()
        return {
            "status": 200,
            "Message": 'Leave Arrival Done Successfully.',
        }

    except (Exception, psycopg2.DatabaseError) as error:
        print("error==leave apply requestt==", error)
        # add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, error, 'Failed')
        return error
