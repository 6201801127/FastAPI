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
# from .auth.security import validate_token
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


router = APIRouter()


@router.post('/login/')
async def userlogin(request: Request, request_param: dict = Body(...)):
    pranNo = request_param.get('pranNo')
    engine = ''
    try:
        engine = connectToDB()
        engine.autocommit = False
        cur = engine.cursor()
        date_time = datetime.now(pytz.timezone('Asia/Kolkata')) + relativedelta(months=1)
        date_time = date_time.strftime("%Y-%m-%d %H:%M:%S")
        if engine and pranNo:
            qdata = cur.execute(f"select id,work_phone,user_id,name from hr_employee where gpf_no = '{pranNo}';")
            rows = cur.fetchall()
            if not rows:
                qdata = cur.execute(f"select id,work_phone,user_id,name from hr_employee where employee_login_id = '{pranNo}';")
                rows = cur.fetchall()
            print(rows, "7&&&&&&&&&&")
            date_time = False
            ip_address = request.client.host
            date_time = datetime.now(pytz.timezone('Asia/Kolkata')) + relativedelta(months=1)
            date_time = date_time.strftime("%Y-%m-%d %H:%M:%S")
            if rows:
                try:
                    # GENERATE OTP
                    otp_value = ''.join(random.choice(string.digits) for _ in range(4))
                    print("OTP is : ", otp_value)
                    # 10 minute OTP expiry duration
                    # Insert New token to api_access_token log
                    # insert in OTP log
                    otp_log = cur.execute(f"""insert into bsap_generate_otp (user_id,mobile_no,otp,exp_date_time,create_date,write_date) 
                    values('{rows[0][0]}','{rows[0][1]}','{otp_value}','{date_time}','{datetime.now()}','{datetime.now()}')""")
                    engine.commit()
                    """ 1) Send OTP TO User
                        2) update login details 
                        3) Update API log
                    """
                    login_query = cur.execute(f"""insert into user_login_detail (user_id,name,ip_address,status,create_date,write_date,create_uid,write_uid,date_time) 
                    values('{rows[0][2]}','{rows[0][3]}','{ip_address}','Success','{datetime.now()}','{datetime.now()}','{rows[0][2]}','{rows[0][2]}','{date_time}')""")
                    engine.commit()
                    response_data = {
                        "status": 200,
                        "message": "OTP sent successfully",
                        "mobileNo": '******' + rows[0][1][-4:],
                        "otp": otp_value,
                    }
                    add_api_log(date_time, request_param, response_data, 'Success')
                    engine.commit()
                    return response_data
                except (Exception, psycopg2.DatabaseError) as error:
                    login_query = cur.execute(f"""insert into user_login_detail (user_id,name,ip_address,status,create_date,write_date,create_uid,write_uid) 
                    values('{rows[0][2]}','{rows[0][3]}','{ip_address}','Failed','{datetime.now()}','{datetime.now()}','{rows[0][2]}','{rows[0][2]}')""")
                    engine.commit()
                    response_data = {"status": 401, "error": "Invalid Authentication."}
                    add_api_log(date_time, request_param, response_data, 'Failed')
                    engine.commit()
                    return response_data
            else:
                response_data = {"status": 401, "error": "Invalid Authentication."}
                add_api_log(date_time, request_param, response_data, 'Failed')
                return response_data
        else:
            response_data = {"status": 403, "error": "Something went wrong. Please try again later."}
            add_api_log(date_time, request_param, response_data, 'Failed')
            return response_data
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error in transction Reverting all other operations of a transction ", error)
        engine.rollback()
        add_api_log(date_time, request_param, response_data, 'Failed')
        return {
            'status': 403,
            "message": str(error)
        }
    finally:
        if engine:
            # closing database connection.

            engine.close()
            print("PostgreSQL connection is closed")
            if cur:
                cur.close()
                print("CURSER CLOSED")


""" Insert in API log to maintain API status """

@router.post('/verifyUser/')
async def verifyuser(request: dict = Body(...)):
    try:
        deviceId, otp, pranNo = request.get('deviceId'), request.get('otp'), request.get('pranNo')
        date_time = datetime.now(pytz.timezone('Asia/Kolkata')) + relativedelta(years=1)
        date_time = date_time.strftime("%Y-%m-%d %H:%M:%S")
        engine = connectToDB()
        engine.autocommit = False
        cur = engine.cursor()
        if engine and deviceId and otp and pranNo:


            qdata = cur.execute(
                f"select id,work_phone,user_id,name,work_email from hr_employee where gpf_no = '{pranNo}';")
            rows = cur.fetchall()
            if not rows:
                qdata = cur.execute(
                    f"select id,work_phone,user_id,name,work_email from hr_employee where employee_login_id = '{pranNo}';")
                rows = cur.fetchall()
            # if not rows:
            #     return {
            #         'Status':200,
            #         "Message": 'Wrong '
            #     }
            data = False
            # userId =  return_employee_data()
            emp_query = cur.execute(
                f"""SELECT otp,user_id FROM bsap_generate_otp WHERE user_id = '{rows[0][0]}' ORDER BY id DESC LIMIT 1;""")
            emp_data = cur.fetchall()
            if emp_data[0][0] == otp:
                encode_token = signJWT(rows[0][1], rows[0][2], rows[0][0], emp_data[0][0]).get('token')
                print("encode_token===", encode_token)
                # Inser into logout_user table
                logout_log = cur.execute(
                    f"insert into logout_user (employee_id,logoutuser,create_date,write_date) values('{rows[0][0]}','True','{datetime.now()}','{datetime.now()}')")
                # Insert New token to api_access_token log
                token_log = cur.execute(
                    f"insert into api_access_token (token,user_id,expires,create_date,write_date) values('{str(encode_token)[2:-1]}','{rows[0][2]}','{date_time}','{datetime.now()}','{datetime.now()}')")
                data = {
                    "status": 200,
                    "message": "success",
                    "token": encode_token,
                }
                # if ex_friend_data:
                qdata = cur.execute(f'''select a.id,a.name,j.name as designation,a.current_office_id,b.name,a.work_phone,a.work_location,a.gpf_no,a.work_email,c.street,c.street2,e.name as city,f.name as state,g.name as country,c.zip,d.joining_charge_taken_date,a.parent_id 
                                    from hr_employee a join res_branch b on a.current_office_id = b.id 
                                    join employee_address c on a.id = c.employee_id 
                                    join hr_employee_joining_detail d on a.id = d.employee_id 
                                    join hr_job j on a.job_id = j.id
                                    join res_city e on c.city_id = e.id 
                                    join res_country_state f on c.state_id = f.id
                                    join res_country g on c.country_id = g.id
                                    where a.id =  '{emp_data[0][1]}';''')
                rows = cur.fetchall()
                # print("rows=========", rows)
                if not rows:
                    data['userDetails'] = {}
                if rows:
                    for row in rows:
                        # print("row-----", row)
                        base_url = f"{config('base_url', default='http://192.168.61.86:8069/')}web/image?model=hr.employee&field=image_1920&id={row[0]}&unique="
                        face_query = cur.execute(
                            f"select face_info from hr_employee_face_info where employee_id = '{row[0]}' and face_info is not null")
                        face_data = cur.fetchall()
                        finger_query = cur.execute(
                            f"select finger_info from hr_employee_face_info where employee_id = '{row[0]}' and finger_info is not null")
                        finger_data = cur.fetchall()
                        # print("row[0]===", row[0])
                        is_ra_data = cur.execute(f"select id from hr_employee where parent_id = '{row[0]}'")
                        ra_data = cur.fetchall()
                        ra_val = False
                        if row[16] != None:
                            ra_val_data = cur.execute(f"select name from hr_employee where id = '{row[16]}'")
                            ra_val = cur.fetchall()
                        # print("finger_data=", finger_data)
                        data['userDetails'] = {
                            "userId": row[0],
                            "name": row[1],
                            "designation": row[2],
                            "pranNo": row[7],
                            "deptId": row[3],
                            "department": row[4],
                            "faceRegistered": 1,
                            "faceData": face_data[0][0] if face_data else "",
                            "fingerData": finger_data[0][0] if finger_data else "",
                            "profilePic": base_url if base_url else None,
                            "mobileNo": row[5],
                            "postingLocation": row[6],
                            'isRA': True if ra_data else False,
                            'email': row[8],
                            'address': f"""{row[9] if row[9] != None else ''} {row[10] if row[10] != None else ''} {row[11] if row[11] != None else ''} {row[12] if row[12] != None else ''}, Zip= {row[13] if row[13] != None else ''},Pin= {row[14] if row[14] != None else ''}""",
                            'joiningDate': row[15],
                            'ra': ra_val[0][0] if ra_val else ''
                        }

                emp_leave_query = cur.execute(
                    f"""SELECT COUNT(id) FROM hr_leave WHERE employee_id = '{rows[0][0]}' and state = 'validate';""")
                emp_leave_data = cur.fetchall()
                print(emp_leave_data, 'emp_leave_data')
                if emp_leave_data:
                    data['userDetails']['leave_data'] = emp_leave_data[0][0]
                if not emp_leave_data:
                    data['userDetails']['leave_data'] = 0

                employee_query = cur.execute(
                    f"""SELECT COUNT(id) as total FROM hr_employee ;""")
                employee_data = cur.fetchall()
                print(employee_data, 'employee_data')
                if employee_data:
                    data['userDetails']['employee_data'] = employee_data[0][0]
                if not employee_data:
                    data['userDetails']['employee_data'] = 0

                employee_grivance_query = cur.execute(
                    f"""SELECT COUNT(id) FROM bsap_grievance  where employee_id = '{rows[0][0]}';""")
                employee_grievance_data = cur.fetchall()
                print(employee_grievance_data, 'employee_grievance_data')
                if employee_grievance_data:
                    data['userDetails']['grivance_data'] = employee_grievance_data[0][0]
                if not employee_grievance_data:
                    data['userDetails']['grivance_data'] = 0

                emp_eorderly_query = cur.execute(
                    f"""SELECT COUNT(id) FROM bsap_vigilance  where name_of_complaint = '{rows[0][0]}';""")
                emp_eorderly_data = cur.fetchall()
                print(emp_eorderly_data, 'emp_eorderly_data')
                if emp_eorderly_data:
                    data['userDetails']['eorderly_data'] = emp_eorderly_data[0][0]
                if not emp_eorderly_data:
                    data['userDetails']['eorderly_data'] = 0

                emp_erequest_query = cur.execute(
                    f"""SELECT COUNT(id) FROM bsap_erequest  where employee_id = '{rows[0][0]}';""")
                emp_erequest_data = cur.fetchall()
                print(emp_erequest_data, 'emp_erequest')

                if emp_erequest_data:
                    data['userDetails']['erequest_data'] = emp_erequest_data[0][0]
                if not emp_erequest_data:
                    data['userDetails']['erequest_data'] = emp_erequest_data[0][0]



                engine.commit()
                add_api_log(date_time, request, data, 'Success')

                return data
            else:
                data = {"status": 401, "message": "Invalid OTP."}
                add_api_log(date_time, request, data, 'Failed')
                engine.rollback()
                return data
        else:
            data = {"status": 403, "message": "Something went wrong. Please try again later."}
            add_api_log(date_time, request, data, 'Failed')
            engine.rollback()
            return data

    except (Exception, psycopg2.DatabaseError) as error:
        # print("error==7777777777777777777777777777==", error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, error, 'Failed')
        # engine.rollback()
        return {
            "status": 403,
            "message": str(error)
        }
    finally:
        if engine:
            engine.close()
            print("PostgreSQL connection is closed")
            if cur:
                cur.close()
                print('CURSOR CLOSED')


@router.post('/verifyAndTagFriend/', dependencies=[Depends(validate_token)])
async def verifyandtagfriend(request: dict = Body(...)):
    try:
        friendId, otp, deviceId = request.get('friendId'), request.get('otp'), request.get('deviceId')
        date_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        date_time = date_time.strftime("%Y-%m-%d %H:%M:%S")
        engine = connectToDB()
        engine.autocommit = False
        cur = engine.cursor()
        if engine and friendId and otp:
            emp_query = cur.execute(
                f"""SELECT otp,user_id FROM bsap_generate_otp WHERE user_id = '{friendId}' ORDER BY id DESC LIMIT 1;""")
            emp_data = cur.fetchall()
            # print(emp_data, '&&&&&&&&')
            userId = return_employee_data()
            print(userId, '********')
            # employee_otp = userId.get('otp')
            # print("emp_data=", emp_data)
            employee_otp = emp_data[0][0]
            # print("employee_otp=", employee_otp)
            user_id = userId.get('employeeId')
            if employee_otp == otp:
                if userId:
                    if userId.get('userId') == emp_data[0][1]:
                        # print('NOOOOOOOOOO')
                        face_log_query = cur.execute(f"""insert into friend_register_log (user_id,device_id,friend_id,year,month,create_date,write_date) 
                        values('{emp_data[0][1]}','{deviceId}','{friendId}','{datetime.today().year}','{datetime.today().month}','{date_time}','{date_time}')""")
                    if userId.get('employeeId') != emp_data[0][1]:
                        # print('HELLLO')
                        face_log_query = cur.execute(f"""insert into friend_register_log (user_id,device_id,friend_id,year,month,create_date,write_date) 
                                            values('{user_id}','{deviceId}','{friendId}','{datetime.today().year}','{datetime.today().month}','{date_time}','{date_time}')""")

                engine.commit()
                data = {
                    "status": 200,
                    "message": "success",
                }
                add_api_log(date_time, request, data, 'Success')
                return data

            else:
                data = {"status": 401, "message": "Invalid OTP."}
                add_api_log(date_time, request, data, 'Failed')
                engine.rollback()
                return data
        else:
            data = {"status": 403, "message": "Something went wrong. Please try again later."}
            add_api_log(date_time, request, data, 'Failed')
            engine.rollback()
            return data
    except (Exception, psycopg2.DatabaseError) as error:
        # print("error==777777777777777777777777777777777==", error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, error, 'Failed')
        engine.rollback()
        return {
            "status": 403,
            "message": str(error)
        }
    finally:
        if engine:
            engine.close()
            print("PostgreSQL connection is closed")
            if cur:
                cur.close()
                print("CURSOR CLOSED")

@router.post('/registerFace/', dependencies=[Depends(validate_token)])
async def registerface(request: dict = Body(...)):
    try:
        engine = connectToDB()
        engine.autocommit = False
        cur = engine.cursor()
        friendId, faceData, action, deviceId = request.get('friendId'), request.get('faceData'), request.get(
            'action'), request.get('deviceId')
        userId = False
        if friendId:
            userId = friendId
        else:
            user_dict = return_employee_data()
            userId = user_dict.get('employeeId')
        # print("userId===55555--------------------", userId)
        date_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        date_time = date_time.strftime("%Y-%m-%d %H:%M:%S")
        if engine and faceData and action:

            data = False
            date_val = date.today()
            if action == 'R':
                check_ex_data = cur.execute(
                    f"select employee_id from hr_employee_face_info where employee_id = '{userId}'")
                ex_data = cur.fetchall()
                if ex_data:
                    update_face_data(faceData, deviceId, date_val, userId, date_time)
                    data = {
                        "status": 200,
                        "message": "Updated successfully",
                    }
                else:
                    reg_face_query = cur.execute(f"""insert into hr_employee_face_info (face_info,device_id,type,employee_id,remarks,create_date,write_date) 
                    values('{faceData}','{deviceId}','face','{userId}','New Data Added on {date_val}','{date_time}','{date_time}')""")
                    data = {
                        "status": 200,
                        "message": "Register successfully",
                    }
                    engine.commit()
            elif action == 'U':
                update_face_data(faceData, deviceId, date_val, userId, date_time)
                data = {
                    "status": 200,
                    "message": "Updated successfully",
                }
            else:
                data = {
                    "status": 401,
                    "error": "Invalid Authentication."
                }
                engine.rollback()
        else:
            data = {
                "status": 403,
                "error": "Something went wrong. Please try again later."
            }
            engine.rollback()

        add_api_log(date_time, request, data, 'Success')
        return data
    except (Exception, psycopg2.DatabaseError) as error:
        # print("error==666666666666666666666666666666==", error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, error, 'Failed')
        engine.rollback()
        return {
            "status": 403,
            "messsage": str(error)
        }
    finally:
        if engine:
            cur.close()
            engine.close()
            print("PostgreSQL connection is closed")


""" After successfully login user need to register his/ her face data. """


async def update_face_data(faceInfo: Optional[str] = None, deviceId: Optional[str] = None,
                           date_val: Optional[str] = None, userId: Optional[str] = None,
                           date_time: Optional[str] = None):
    engine = connectToDB()
    if engine:
        cur = engine.cursor()
        upd_face_query = cur.execute(
            f"""update hr_employee_face_info set face_info = '{faceInfo}',device_id = '{deviceId}',
        remarks = 'Data Updated on {date_val}', create_date = '{date_time}', write_date = '{date_time}' 
        where type = 'face' and employee_id = '{userId}'""")
        engine.commit()
        cur.close()





@router.post('/getEmployeeDirectory/', dependencies=[Depends(validate_token)])
async def getemployeedirectory(request: dict = Body(...)):
    try:
        engine = connectToDB()
        engine.autocommit = False
        cur = engine.cursor()
        query, pagesize, = request.get('query'), request.get('pageSize')
        pageno = (int(request.get('pageNo')) - 1) * int(request.get('pageSize')) if int(
            request.get('pageNo')) > 0 else 0
        # print("pageno", pageno)
        if engine and pagesize:
            qdata = False
            if query:
                qdata = cur.execute(f'''select a.id,a.name,a.gpf_no,d.joining_charge_taken_date as joining_date ,j.name as designation,
                b.name as department,a.work_location,a.parent_id,e.street as address,a.work_email,a.work_phone
                                        from hr_employee a join hr_department b on a.department_id = b.id 
                                        join hr_job j on a.job_id = j.id
                                        join hr_employee_joining_detail d on a.id = d.employee_id
                                        join employee_address e on a.id = e.employee_id
                                        where a.name ilike '%{query}%' or a.gpf_no ilike '%{query}%' 
                                        order by a.id asc offset '{pageno}' limit '{pagesize}';''')
            else:
                qdata = cur.execute(f'''select a.id,a.name,a.gpf_no,d.joining_charge_taken_date as joining_date ,j.name as designation,
                b.name as department,a.work_location,a.parent_id,e.street as address,a.work_email,a.work_phone
                                        from hr_employee a join hr_department b on a.department_id = b.id 
                                        join hr_job j on a.job_id = j.id
                                        join hr_employee_joining_detail d on a.id = d.employee_id
                                        join employee_address e on a.id = e.employee_id
                                        order by a.id asc offset '{pageno}' limit '{pagesize}';''')
            rows = cur.fetchall()
            # print("rows", rows)
            employee_id = return_employee_data().get('employeeId')
            print(employee_id, return_employee_data(), '*******')
            emp_friend_data = cur.execute(
                f"""select friend_id from friend_register_log where user_id = '{employee_id}'; """)
            f_data = cur.fetchall()
            # print("friend_data2=", [rec[0] for rec in set(f_data)])
            friend_data = [employee_id] + [rec[0] for rec in set(f_data)]
            # print(friend_data, 'FFFFFFFFFFF')
            # emp_own_data = cur.execute(
            #     f"""select friend_id from friend_register_log where friend_id = '{employee_id}'; """)
            # e_data = cur.fetchall()
            # if e_data:
            #     friend_data = friend_data + [rec[0] for rec in set(e_data)]
            print(friend_data, 'FRIEND DATA')
            # query = "select employee_id from hr_employee_face_info where employee_id"
            # if len(friend_data) == 1:
            #     print('deepak')
            #     query = query + "= " + str(friend_data[0])
            #     print(query, 'QQQQQQQQQQRRRRRRRRR')
            # if len(friend_data) > 1:
            #     query = query + f""" in {tuple(friend_data)}"""
            #     print(query, 'QQQQQQQQQQ')
            # print(query, 'DDQQQQQQQQQQ')
            # emp_face_data = cur.execute(query)
            # emp_face_data = cur.execute(
            #     f"""select employee_id from hr_employee_face_info where employee_id in {tuple(friend_data)};"""
            # )
            # face_data = cur.fetchall()
            # face_emp_list = [data[0] for data in set(face_data)]

            if rows:
                friend_list = []
                for row in rows:
                    emp_name = False
                    if row[7]:
                        emp_data = cur.execute(f"select name from hr_employee where id = '{row[7]}';")
                        emp_name = cur.fetchall()
                    base_url = f"{config('base_url', default='http://192.168.61.86:8069/')}web/image?model=hr.employee&field=image_1920&id={row[0]}&unique="
                    if row[0]:
                        emp_face_data = cur.execute(f"select id, employee_id from  hr_employee_face_info where employee_id = '{row[0]}';")
                        emp_face_data_register = cur.fetchall()
                    data_list = {
                        "id": row[0] if row[0] != None else '',
                        "empName": row[1] if row[1] != None else '',
                        "pranNo": row[2] if row[2] != None else '',
                        "dateOfJoining": row[3].strftime("%d-%m-%Y") if row[3] != None else '',
                        "designation": row[4] if row[4] != None else '',
                        "department": row[5] if row[5] != None else '',
                        "postingLocation": row[6] if row[6] != None else '',
                        "reportingAuthority": emp_name[0][0] if emp_name else '',
                        "address": row[8] if row[8] != None else '',
                        "email": row[9] if row[9] != None else '',
                        "phone": row[10] if row[10] != None else '',
                        "profilePic": base_url,
                        "isUsersFriend": True if row[0] in friend_data else False,
                        "isFaceRegister": True if emp_face_data_register else False,
                    }
                    friend_list.append(data_list)
                # print(friend_list)
                engine.commit()
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request,
                            friend_list, 'Success')
                return friend_list
            else:
                engine.rollback()
                return []
        else:
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, [],
                        'Failed')
            engine.rollback()
            return {"status": 403, "error": "Something went wrong. Please try again later."}
    except (Exception, psycopg2.DatabaseError) as error:
        # print("error==5555555555555555555555555555555555==", error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, error, 'Failed')
        engine.rollback()
        return {
            "status": 403,
            "message": str(error)
        }
    finally:
        if engine:
            cur.close()
            engine.close()
            print("PostgreSQL connection is closed")


@router.post('/getUserFriendsInfo/', dependencies=[Depends(validate_token)])
async def getuserfriendsinfo(request: dict = Body(...)):
    try:
        engine = connectToDB()
        engine.autocommit = False
        cur = engine.cursor()
        year, month, pranNo = request.get('year'), request.get('month'), request.get('pranNo')
        if engine and year and month:

            # try:
            data = {
                "status": 200,
                "message": "success",
            }
            employee_data = return_employee_data()
            otp = employee_data.get('otp')
            employee_id = employee_data.get('employeeId')
            print("employee_id------------", employee_id)
            friend_data = False
            if pranNo:
                emp_friend_data = cur.execute(f"""select id from hr_employee where gpf_no = '{pranNo}'; """)
                f_data = cur.fetchall()
                if not f_data:
                    emp_friend_data = cur.execute(f"""select id from hr_employee where employee_login_id = '{pranNo}'; """)
                    f_data = cur.fetchall()
                # print("friend_data1=",[rec[0] for rec in set(f_data)])
                friend_data = [rec[0] for rec in set(f_data)]
            else:
                emp_friend_data = cur.execute(
                    f"""select friend_id from friend_register_log where user_id = '{employee_id}'; """)
                f_data = cur.fetchall()
                # print("friend_data2=", [rec[0] for rec in set(f_data)])
                print("friend_data2=", f_data)
                remove_dublicate_id_data = set(f_data)
                friend_data = [employee_id] + [rec[0] for rec in set(f_data)]
                friend_data = list(set(friend_data))
                print(friend_data, 'DDDDDDDDD')
            # print("list data---------------------------------------------------------------",set(friend_data))
            if friend_data:
                data_list = []
                for friend in set(friend_data):
                    emp_data = False
                    emp_query = cur.execute(f""" select a.id,a.name,a.gpf_no,j.name as designation,a.work_phone
                                                from hr_employee a join hr_job j on a.job_id = j.id
                                                where a.id = {friend}; """)
                    rows_data = cur.fetchall()
                    print('rows', rows_data)
                    if rows_data:
                        for rows in rows_data:
                            face_query = cur.execute(
                                f"select face_info from hr_employee_face_info where employee_id = '{rows[0]}' and face_info is not null")
                            face_data = cur.fetchall()
                            # print("face_data",face_data)
                            base_url = f"{config('base_url', default='http://192.168.61.86:8069/')}web/image?model=hr.employee&field=image_1920&id={rows[0]}&unique="
                            emp_data = {
                                "id": rows[0] if rows[0] is not None else '',
                                "empName": rows[1] if rows[1] is not None else '',
                                "pranNo": rows[2] if rows[2] is not None else '',
                                "faceData": face_data[0][0] if face_data else '',
                                "designation": rows[3] if rows[3] is not None else '',
                                "phone": rows[4] if rows[4] is not None else '',
                                "profilePic": base_url if base_url else ''
                            }
                            # employee_data = cur.execute(f"select id,user_id,name from hr_employee where id = '{rows[0]}';")
                            # emp = cur.fetchall()
                            # print("No", rows, rows[0], year, month, datetime.now(), str(month.lstrip('0')))
                            leave_data = cur.execute(f"""select to_date(to_char(date_from, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
                            to_date(to_char(date_to, 'YYYY-MM-DD'), 'YYYY-MM-DD') from hr_leave 
                            where employee_id = '{rows[0]}' and to_char(date_trunc('month', date_from),'MM') = '{str(month)}' 
                            and to_char(date_trunc('year', date_from),'YYYY') = '{str(year)}';""")
                            leaves = cur.fetchall()
                            present_attn_data = cur.execute(f"""select atten_date from hr_attendance 
                            where employee_id = '{rows[0]}' and year = '{str(year)}' and month = '{str(month.lstrip('0'))}' and state in ('0','1'); """)
                            present_data = cur.fetchall()
                            # print("present_data=", present_data)
                            absent_attn_data = cur.execute(f"""select atten_date from hr_attendance 
                            where employee_id = '{rows[0]}' and year = '{str(year)}' and month = '{str(month.lstrip('0'))}' and state='2'; """)
                            absent_data = cur.fetchall()
                            # print("absent_data=", absent_data)
                            late_data = cur.execute(f""" select atten_date from hr_attendance 
                            where employee_id = '{rows[0]}' and year = '{str(year)}' and month = '{str(month.lstrip('0'))}' and state ='0'; """)
                            lates = cur.fetchall()
                            # print('lates', lates)
                            leave_dt = []
                            if leaves:
                                for leave in leaves:
                                    from_dt, to_dt = leave[0], leave[1]
                                    print("from_dt,to_dt", from_dt, to_dt)
                                    leave_dt += [datetime.strftime(from_dt + timedelta(days=x), '%d-%m-%Y') for x in
                                                 range((to_dt - from_dt).days)] + [datetime.strftime(to_dt, '%d-%m-%Y')]
                            roster_data = cur.execute(f"""select date from bsap_employee_roaster_shift 
                            where employee_id = '{rows[0]}' and to_char(date_trunc('month', date),'MM') =  '{str(month)}' 
                            and to_char(date_trunc('year', date),'YYYY') = '{str(year)}' and week_off_status = True;""")
                            roster = cur.fetchall()
                            # print('roster',roster)
                            week_offs = False
                            if roster:
                                week_offs = roster
                            else:
                                weekoffs_data = cur.execute(f""" select l.start_date from hr_employee a 
                                join resource_calendar b on a.calendar_id = b.id 
                                join resource_calendar_leaves l on b.id = l.calendar_id 
                                where a.id =  {rows[0]} and to_char(date_trunc('month', l.start_date),'MM') = '{str(month)}'
                                and to_char(date_trunc('year', l.start_date),'YYYY') = '{str(year)}' """)
                                week_offs = cur.fetchall()

                            """ Fetch the holidays
                                1) Holidays is fixes in shifts for all shift types
                            """
                            holidays_data = cur.execute(f""" select date from hr_holidays_public_line 
                            where to_char(date_trunc('month', date),'MM') = '{str(month)}'and to_char(date_trunc('year', date),'YYYY') = '{str(year)}' """)
                            holidays = cur.fetchall()
                            print("holidays==", holidays, leave_dt)
                            emp_data['attendance'] = {
                                "workingDays": (monthrange(int(year), int(month))[1]) - (
                                            len([datetime.strftime(day[0], '%d-%m-%Y') for day in
                                                 week_offs] if week_offs else '') + len(
                                        [datetime.strftime(holiday[0], '%d-%m-%Y') for holiday in
                                         holidays] if holidays else '')),
                                "present": len([present[0] for present in present_data]) if present_data else 0,
                                "late": len([datetime.strftime(late_entry[0], '%d-%m-%Y') for late_entry in
                                             lates]) if lates else 0,
                                "absent": len([absent[0] for absent in absent_data]) if absent_data else 0,
                                "leave": len(leave_dt) if leave_dt else 0
                            }
                            friend_attn_data = cur.execute(f"""select atten_date,check_in,check_out,lat,lng,location,
                            remark,cout_remark,cout_lat,cout_lng from hr_attendance where employee_id = '{rows[0]}' 
                            and year = '{str(year)}' and month = '{str(month)}' order by atten_date desc; """)
                            friend_attn_data = cur.fetchall()
                            print("friend_attn_data==",friend_attn_data)
                            if friend_attn_data:
                                report_list = []
                                for attn in friend_attn_data:
                                    report = {
                                        "date": attn[0].strftime("%d-%m-%Y") if attn[0] is not None else '',
                                        "checkIn": attn[1].strftime("%d-%m-%Y %H:%M:%S") if attn[1] is not None else '',
                                        "checkOut": attn[2].strftime("%d-%m-%Y %H:%M:%S") if attn[
                                                                                                 2] is not None else '',
                                        "status": "present" if attn[1] is not None else 'absent',
                                        "checkin_lat": attn[3] if attn[3] is not None else '',
                                        "checkin_lng": attn[4] if attn[4] is not None else '',
                                        "checkin_remark": attn[6] if attn[6] is not None else '',
                                        "checkout_lat": attn[7] if attn[7] is not None else '',
                                        "checkout_lng": attn[8] if attn[8] is not None else '',
                                        "checkout_remark": attn[9] if attn[9] is not None else '',
                                        "address": attn[5] if attn[5] is not None else ''
                                    }
                                    report_list.append(report)
                                emp_data['reports'] = report_list
                            else:
                                emp_data['reports'] = []
                            data_list.append(emp_data)

                    else:
                        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request,
                                    {"status": 401, "message": "No Data found."}, 'Failed')
                        engine.rollback()
                        return {"status": 401, "message": "No Data found11111."}
                data['data'] = data_list
            else:
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request,
                            {"status": 401, "message": "No Data found."}, 'Failed')
                engine.rollback()
                return {"status": 401, "message": "No Data found."}
            engine.commit()
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, data,
                        'Failed')
            return data
        else:
            engine.rollback()
            return {"status": 403, "error": "Something went wrong. Please try again later."}
    except (Exception, psycopg2.DatabaseError) as error:
        # print("error==4444444444444444444444444444==", error)
        engine.rollback()
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, error, 'Failed')
        return {
            "status": 403,
            "message": str(error)
        }
    finally:
        if engine:
            cur.close()
            engine.rollback()
            print("Postgres Database connection is closed completely!!!")

@router.post('/verifyFriendOtp/', dependencies=[Depends(validate_token)])
async def verifyfriendotp(request: dict = Body(...)):
    pranNo, otp = request.get('pranNo'), request.get('pranNo')
    # print("pranNopranNo",pranNo)
    engine = connectToDB()
    if engine and pranNo:
        cur = engine.cursor()
        qdata = cur.execute(f"select id,work_phone,user_id,name from hr_employee where gpf_no = '{pranNo}';")
        rows = cur.fetchall()
        if not rows:
            qdata = cur.execute(f"select id,work_phone,user_id,name from hr_employee where employee_login_id = '{pranNo}';")
            rows = cur.fetchall()
        if rows:
            otp_value = ''.join(random.choice(string.digits) for _ in range(4))
            # print("OTP is : ", otp_value)
            date_time = datetime.now(pytz.timezone('Asia/Calcutta')) + timedelta(0, 600)
            date_time = date_time.strftime("%Y-%m-%d %H:%M:%S")
            ######### Getting HTML Content Data #########
            sms_rendered_query = cur.execute(
                f"""select s.sms_html,g.gateway_url from send_sms s join gateway_setup g on s.gateway_id = g.id where s.name ='Employee_OTP'""")
            sms_rendered_content = cur.fetchall()
            # print("sms_rendered_content",sms_rendered_content)
            #  send SMS
            send_otp = send_sms_link(sms_rendered_content, rows[0][1], date_time, otp_value, rows[0][3])
            # print("send_otp==",send_otp)
            cur.close()
            return send_otp
            # insert in OTP log
            # otp_log = cur.execute(f"insert into bsap_generate_otp (user_id,mobile_no,otp,exp_date_time) values('{rows[0][0]}','{rows[0][1]}','{otp_value}','{date_time}')")
            # engine.commit()
            # #Send OTP TO User
            # return {
            #         "status": 200,
            #         "message": "OTP sent successfully",
            #         }
        else:
            return {"status": 401, "error": "Invalid Authentication."}
    else:
        return {"status": 403, "error": "Something went wrong. Please try again later."}


@router.post('/getUserCampLocation/', dependencies=[Depends(validate_token)])
async def getusercamplocation():
    try:
        engine = connectToDB()
        engine.autocommit = False
        cur = engine.cursor()
        if engine:
            employee_data = return_employee_data()
            # print("employee_data==", employee_data)
            if employee_data:
                print("employee_data.get('employeeId')", employee_data.get('employeeId'))
                qdata = cur.execute(f"""select b.location,b.lat,b.lng from hr_employee a 
                join res_branch_camp_location b on a.camp_location = b.id 
                where a.id = '{employee_data.get('employeeId')}';""")
                rows = cur.fetchall()
                if rows:
                    data = {
                        "logoutUser": "false",
                        "campLocation": rows[0][0],
                        "campLatitude": float(rows[0][1]),
                        "campLongitude": float(rows[0][2])
                    }
                    engine.commit()
                    add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {}, data,
                                'Success')
                    return data
                if not rows:
                    data = {
                        "status": 200,
                        "message": "Camp Location is not assigned for this user.",
                        "data": []
                    }
                    add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {}, data,
                                'Success')
                    engine.rollback()
                    return data

            else:
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {},
                            {"status": 401, "error": "Invalid Authentication."}, 'Failed')
                engine.rollback()
                return {"status": 401, "error": "Invalid Authentication."}
        else:
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {},
                        {"status": 403, "error": "Something went wrong."}, 'Failed')
            engine.rollback()
            return {"status": 403, "error": "Something went wrong."}
    except (Exception, psycopg2.DatabaseError) as error:
        # print("error==22222222222222222222==", error)
        engine.rollback()
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {}, error, 'Failed')
        return {
            "status": 403,
            "message": str(error)
        }
    finally:
        if engine:
            cur.close()
            engine.close()
            print("Connection is closed Completely !!!")



@router.post('/updateLogOutUser/')
async def updatelogoutuser(request: dict = Body(...)):
    try:
        engine = connectToDB()
        pranNo, userType, logoutUser = request.get('pranNo'), request.get('userType'), request.get('logoutUser')
        if engine and userType:
            cur = engine.cursor()
            if userType == '1' and pranNo:
                qdata = cur.execute(f"select id,user_id from hr_employee where gpf_no = '{pranNo}';")
                rows = cur.fetchall()
                if not rows:
                    qdata = cur.execute(f"select id,user_id from hr_employee where employee_login_id = '{pranNo}';")
                    rows = cur.fetchall()
                if rows:
                    update_logout_user = cur.execute(
                        f"update logout_user set logoutuser = '{logoutUser}' where employee_id ='{rows[0][0]}';")
                    cur.close()
                    return_data = {"status": 200,
                                   "message": "Logout Successfully." if logoutUser == 'True' else "Login Successfully."}
                    add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request,
                                return_data, 'Success')
                    return return_data
            elif userType == '2' and not pranNo:
                update_logout_user = cur.execute(f"update logout_user set logoutuser = '{logoutUser}';")
                cur.close()
                return_data = {"status": 200,
                               "message": "Logout Successfully." if logoutUser == 'True' else "Login Successfully."}
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request,
                            return_data, 'Success')
                return return_data
            else:
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request,
                            {"status": 401, "error": "Invalid Authentication."}, 'Failed')
                return {"status": 401, "error": "Invalid Authentication."}
        else:
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request,
                        {"status": 403, "error": "Something went wrong. Please try again later."}, 'Failed')
            return {"status": 403, "error": "Something went wrong. Please try again later."}
    except (Exception, psycopg2.DatabaseError) as error:
        # print("error==333333333333333333333333333==", error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, error, 'Failed')
        return error

@router.get("/getAllEmployeeList")
def get_all_emoloyee():
    '''
        provide all the emoloyee woth pran number and name .
    '''
    try:
        engine = connectToDB()

        cur = engine.cursor()
        myquery = cur.execute(
            f"""select gpf_no, name from hr_employee;""")
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
                    'employee_Pran_no': data[0],
                    'employee_name': data[1]
                }
                data_list.append(data_dict)
            response = {
                "status": 200,
                "message": "Data Fetched Sucessfully",
                "data": data_list
            }
        return response
    except (Exception, psycopg2.DatabaseError) as error:
        return {
            "status": 403,
            "message": str(error),
            "data": []
        }