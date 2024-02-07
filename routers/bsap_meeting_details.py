from fastapi import Depends, FastAPI, HTTPException, Request, Body, APIRouter
import requests
import datetime
import psycopg2
from decouple import config
from datetime import datetime
from datetime import timedelta
import json
from .database import connectToDB
from dateutil.relativedelta import relativedelta
import pytz
from .log_file import add_api_log




router = APIRouter()

@router.post("/meeting-details")
def meeting_detals(pranNo: str = Body(..., Embed=True), date: str = Body(..., Embed=True)):
    request_pram = {}
    request_pram['pranNo'] = pranNo
    request_pram['date'] = pranNo

    try:
        engine = connectToDB()

        if not pranNo and not date:
            response_data = {
                "status": 404,
                "isRa": '',
                "pranNo": pranNo,
                "message": "Please provide PranNo and Date",
            }
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),
                        request_pram, response_data,
                        'Failed')
            return response_data

        if engine and pranNo and date:
            cur = engine.cursor()
            employee_data = cur.execute(f"""select id, name from hr_employee where gpf_no = '{pranNo}'; """)
            employee_id = cur.fetchall()
            if not employee_id:
                employee_data = cur.execute(f"""select id, name from hr_employee where employee_login_id = '{pranNo}'; """)
                employee_id = cur.fetchall()
            if not employee_id:
                response_data = {
                    "status": 200,
                    "pranNo": pranNo,
                    "message": "User not found in the Database",
                }
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),
                            request_pram, response_data,
                            'Failed')
                return response_data
            data_list = []
            date = datetime.strptime(date, '%d-%m-%Y').replace(tzinfo=None).strftime("%Y-%m-%d")
            meeting_details = cur.execute(f"""select id, name,meeting_category,kw_start_meeting_date,kw_duration,online_meeting, other_meeting_url, logged_user_id, state, kw_start_meeting_time, meeting_code, start, stop from kw_meeting_events where kw_start_meeting_date >= '{date}' and state = 'confirmed'; """)
            meeting_details_data = cur.fetchall()
            # print(meeting_details_data, 'meeting_details_data')
            if not meeting_details_data:
                res = {
                    "status": 404,
                    "message": "NO meetings found!!"
                }
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),
                            request_pram, res,
                            'Failed')
                return res
            if meeting_details_data:
                for row in meeting_details_data:
                    participants_query = cur.execute(
                        f"""SELECT kw_meeting_events_id, hr_employee_id from hr_employee_kw_meeting_events_rel where kw_meeting_events_id = {row[0]}""")
                    fetch_participants_query = cur.fetchall()
                    # print(fetch_participants_query, 'fetch_participants_query')
                    if not fetch_participants_query:
                        res = {
                            "status": 404,
                            "message": "No meeting Fund or No participants added in the meeting !!!"
                        }
                        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),
                                    request_pram, res,
                                    'Failed')
                        return res
                    if fetch_participants_query:
                        for participants in fetch_participants_query:
                            # print(participants[1], employee_id[0], 'ccccccccccccc')
                            if participants[1] == employee_id[0][0]:

                                data_dict = {
                                    "id": row[0] if row[0] else ' ',
                                    "meeting_title": row[1] if row[1] else ' ',
                                    "meeting_category": row[2] if row[2] else ' ',
                                    "meeting_start_date": row[3] if row[3] else ' ',
                                    "meeting_duration": row[4]+ ' Hrs' if row[4] else ' ',
                                    "meeting_on": row[5] if row[5] else ' ',
                                    "meeting_url": row[6] if row[6] else ' ',
                                    "is_participant_id": participants[1] if participants[1] else ' ',
                                    "participant_name": employee_id[0][1] if employee_id[0][1] else ' ',
                                    "meeting_staus": row[8] if row[8] else ' ',
                                    "meeting_start_time": row[9] if row[9] else ' ',
                                    "meeting_code": row[10] if row[10] else ' ',
                                    "meeting_display_start": pytz.timezone('UTC').localize(row[11]).astimezone(
                                                pytz.timezone('Asia/Kolkata')).replace(tzinfo=None).strftime("%d-%m-%Y %H:%M:%S") if row[11] else "",
                                    "meeting_display_stop": pytz.timezone('UTC').localize(row[12]).astimezone(
                            pytz.timezone('Asia/Kolkata')).replace(tzinfo=None).strftime("%d-%m-%Y %H:%M:%S") if row[
                            12] else "",
                                }
                                oraniser_query = cur.execute(
                                    f"""SELECT name from hr_employee where user_id = {row[7]}""")
                                fetch_oraniser_query = cur.fetchall()
                                if fetch_oraniser_query:
                                    data_dict['meeting_organiser_name'] = fetch_oraniser_query[0][0]

                                data_list.append(data_dict)
            # print(data_list, 'datalist')
            if len(data_list) == 0:
                res = {
                    "status": 404,
                    "message": "No Meeting available for the login user !!!"
                }
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request_pram,
                            res, 'Failed')
                return res
            res = {
                "status": 200,
                "message": "Success",
                "meeting_details": data_list
            }
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request_pram, res,
                        'Failed')
            return res
        else:
            response_data = {
                "status": 404,
                "message": "something wrong with the database conncection or request payload"
            }
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),
                        request_pram, response_data,
                        'Failed')
            return response_data
    except (Exception, psycopg2.DatabaseError) as error:
        print("error==attendance request appled pending requestt==", error)
        # add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request_pram, error,
        #             'Failed')
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request_pram, error, 'Failed')
        return {
            "status": 403,
            "pranNo": pranNo,
            "message": str(error),
        }


@router.post("/cancel-meeting-details")
def meeting_detals(pranNo: str = Body(..., Embed=True), date: str = Body(..., Embed=True)):
    request_pram = {}
    request_pram['pranNo'] = pranNo
    request_pram['date'] = pranNo

    try:
        engine = connectToDB()

        if not pranNo:
            response_data = {
                "status": 404,
                "pranNo": pranNo,
                "message": "Please provide PranNo",
            }
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),
                        request_pram,
                        response_data,
                        'Failed')
            return response_data

        if engine and pranNo and date:
            cur = engine.cursor()
            employee_data = cur.execute(f"""select id, name from hr_employee where gpf_no = '{pranNo}'; """)
            employee_id = cur.fetchall()
            if not employee_id:
                employee_data = cur.execute(f"""select id, name from hr_employee where employee_login_id = '{pranNo}'; """)
                employee_id = cur.fetchall()
            if not employee_id:
                response_data = {
                    "status": 200,
                    "pranNo": pranNo,
                    "message": "User not found in the Database",
                }
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),
                            request_pram,
                            response_data,
                            'Failed')
                return response_data
            data_list = []
            date = datetime.strptime(date, '%d-%m-%Y').replace(tzinfo=None).strftime("%Y-%m-%d")
            meeting_details = cur.execute(f"""select id, name,meeting_category,kw_start_meeting_date,kw_duration,online_meeting, other_meeting_url, logged_user_id, state, kw_start_meeting_time, meeting_code,meeting_cancel_reason, start, stop from kw_meeting_events where kw_start_meeting_date >= '{date}' and state = 'cancelled'; """)
            meeting_details_data = cur.fetchall()
            print(meeting_details_data, 'meeting_details_data')
            if not meeting_details_data:
                res = {
                    "status": 404,
                    "message": "NO meetings found!!"
                }
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),
                            request_pram,
                            res,
                            'Failed')
                return res
            if meeting_details_data:
                for row in meeting_details_data:
                    participants_query = cur.execute(
                        f"""SELECT kw_meeting_events_id, hr_employee_id from hr_employee_kw_meeting_events_rel where kw_meeting_events_id = {row[0]}""")
                    fetch_participants_query = cur.fetchall()
                    print(fetch_participants_query, 'fetch_participants_query')
                    if not fetch_participants_query:
                        res = {
                            "status": 404,
                            "message": "No meeting Fund or No participants added in the meeting !!!"
                        }
                        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),
                                    request_pram,
                                    res,
                                    'Failed')
                        return res
                    if fetch_participants_query:
                        for participants in fetch_participants_query:
                            print(participants[1], employee_id[0], 'ccccccccccccc')
                            if participants[1] == employee_id[0][0]:

                                data_dict = {
                                    "id": row[0] if row[0] else ' ',
                                    "meeting_title": row[1] if row[1] else ' ',
                                    "meeting_category": row[2] if row[2] else ' ',
                                    "meeting_start_date": row[3] if row[3] else ' ',
                                    "meeting_duration": row[4]+ ' Hrs' if row[4] else ' ',
                                    "meeting_on": row[5] if row[5] else ' ',
                                    "meeting_url": row[6] if row[6] else ' ',
                                    "is_participant_id": participants[1] if participants[1] else ' ',
                                    "participant_name": employee_id[0][1] if employee_id[0][1] else ' ',
                                    "meeting_staus": row[8] if row[8] else ' ',
                                    "meeting_start_time": row[9] if row[9] else ' ',
                                    "meeting_code": row[10] if row[10] else ' ',
                                    "meeting_cancel_reason": row[11] if row[11] else ' ',
                                    # "meeting_display_start": row[12] if row[12] else ' ',
                                    "meeting_display_start": pytz.timezone('UTC').localize(row[12]).astimezone(
                                                pytz.timezone('Asia/Kolkata')).replace(tzinfo=None).strftime("%d-%m-%Y %H:%M:%S") if row[12] else "",
                                    # "meeting_display_stop": row[13] if row[13] else ' ',
                                    "meeting_display_stop": pytz.timezone('UTC').localize(row[13]).astimezone(
                            pytz.timezone('Asia/Kolkata')).replace(tzinfo=None).strftime("%d-%m-%Y %H:%M:%S") if row[
                            13] else "",
                                }
                                oraniser_query = cur.execute(
                                    f"""SELECT name from hr_employee where user_id = {row[7]}""")
                                fetch_oraniser_query = cur.fetchall()
                                if fetch_oraniser_query:
                                    data_dict['meeting_organiser_name'] = fetch_oraniser_query[0][0]

                                data_list.append(data_dict)
            print(data_list, 'datalist')
            if len(data_list) == 0:
                res = {
                    "status": 404,
                    "message": "No Meeting available for the login user !!!"
                }
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request_pram,
                            res,
                            'Failed')
                return res
            res = {
                "status": 200,
                "message": "Success",
                "meeting_details": data_list
            }
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request_pram, res,
                        'Failed')

            return res

        else:

            res = {
                "status": 404,
                "message": "something wrong with the database conncection or request payload"
            }
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request_pram, res,
                        'Failed')

            return res

    except (Exception, psycopg2.DatabaseError) as error:
        print("error==attendance request appled pending requestt==", error)
        # add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request_pram, error,
        #             'Failed')

        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request_pram, error,
                    'Failed')
        return {
            "status": 403,
            "isRa": '',
            "pranNo": pranNo,
            "message": str(error),
            "data": []
        }

