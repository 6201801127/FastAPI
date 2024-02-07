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

@router.post("/grievanceList")
def grivance_list(pranNo: str = Body(..., Embed=True), date: str = Body(..., Embed=True), ):
    '''
        api for return grivance data on the basis of date and pranno.
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

        if not pranNo or not date:
            d = datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d')
            response_data = {
                "status": 404,
                "isRa": '',
                "pranNo": pranNo,
                "message": "Please provide PranNo and Date",
            }
            return response_data

        if engine and pranNo and date:
            cur = engine.cursor()
            response_data = {
                "status": 200,
                "isRa": False,
                "pranNo": pranNo,
                "message": "Successfull",
            }
            start_date, end_date = date_match(date)
            print(start_date, end_date, 'start_date, end_date ')
            employee_data = cur.execute(f"""select id from hr_employee where gpf_no = '{pranNo}'; """)
            employee_id = cur.fetchall()
            if not employee_id:
                employee_data = cur.execute(f"""select id from hr_employee where employee_login_id = '{pranNo}'; """)
                employee_id = cur.fetchall()
            if not employee_id:
                response_data = {
                    "status": 200,
                    "isRa": '',
                    "pranNo": pranNo,
                    "message": "User not found in the Database",
                    "data": []
                }
                return response_data
            print(employee_id, 'employee_id')
            if employee_id[0][0]:
                isra = cur.execute(f"""select id from hr_employee where parent_id = {employee_id[0][0]}; """)
                isra_result = cur.fetchall()
                print(isra, isra_result, '*&*&*&*&*&*')

            if isra_result:
                response_data['isRa'] = True

            # backup query
            # exising_request = cur.execute(f"""select bg.id as grievace_id, bg.grievance_code as grievace_no, he.name as employee_name, bg.department as employee_department, bg.designation as employee_designation, rb.name as employee_currentOffice, he.unit as employee_currentUnit, bgt.name as grievace_type,bg.state as grievace_status, bg.subject as purpose, hee.name as action_to_be_taken_by, bg.create_date as created_date from bsap_grievance bg  join hr_employee he on he.id = {employee_id[0][0]}  JOIN hr_department hd on hd.id = he.department_id join res_branch rb on rb.id = he.current_office_id join hr_employee hee on hee.id = bg.so_action_taken_by join bsap_grievance_type bgt on bgt.id = grievance_type_id  where bg.create_date::DATE =  '{d}' """)
            exising_request = cur.execute(
                f"""select bg.id as grievace_id, bg.grievance_code as grievace_no, he.name as employee_name, bg.department as employee_department, bg.designation as employee_designation, rb.name as employee_currentOffice, he.unit as employee_currentUnit, bgt.name as grievace_type,bg.state as grievace_status, bg.subject as purpose, bg.create_date as created_date from bsap_grievance bg  join hr_employee he on he.id = {employee_id[0][0]}  JOIN hr_department hd on hd.id = he.department_id join res_branch rb on rb.id = he.current_office_id  join bsap_grievance_type bgt on bgt.id = grievance_type_id  where bg.create_date >=  '{start_date}' and bg.create_date <=  '{end_date}' and bg.state != '{"draft"}' order by bg.id desc""")
            exising_request_data = cur.fetchall()
            greviance_ids = []
            print("DEEPAK", exising_request_data)
            if exising_request_data:
                grievance_status_dict = {
                    'apply': 'Applied',
                    'receive': 'Received',
                    'in_progress': 'In Progress',
                    'verify': 'Verified',
                    'approve': 'FO Pending',
                    'closed': 'Closed',
                    'reject': 'Rejected',
                    'reset': 'Reset'
                }
                for row in exising_request_data:
                    greviance_ids.append(row[0])
                    key = row[8] if row[8] else ' '
                    data_dict = {
                        "grievace_id": str(row[0]) if row[0] else ' ',
                        "grievace_no": row[1] if row[1] else ' ',
                        "employee_name": row[2] if row[2] else ' ',
                        "employee_department": row[3] if row[3] else ' ',
                        "employee_designation": row[4] if row[4] else ' ',
                        "employee_currentOffice": row[5] if row[5] else ' ',
                        "employee_currentUnit": row[6] if row[6] else ' ',
                        "grievace_type": row[7] if row[7] else ' ',
                        "grievace_status": grievance_status_dict[key],
                        "purpose": row[9] if row[9] else ' ',
                        "action_to_be_taken_by": '',
                        "created_date": row[10].date().strftime("%d-%m-%Y"),
                        "created_date": (row[10] + timedelta(minutes=330)).strftime('%d-%m-%Y'),
                    }
                    data.append(data_dict)

                if len(greviance_ids) == 1:
                    check_isra = cur.execute(
                        f"""select bg.id, hee.name as action_to_be_taken_by from bsap_grievance as bg join hr_employee hee on hee.id = bg.so_action_taken_by where bg.id = {greviance_ids[0]} """)
                if len(greviance_ids) > 1:
                    check_isra = cur.execute(
                        f"""select bg.id, hee.name as action_to_be_taken_by from bsap_grievance as bg join hr_employee hee on hee.id = bg.so_action_taken_by where bg.id in {tuple(greviance_ids)} """)
                check_isra_result = cur.fetchall()
                print(check_isra_result, '$$$$$$$$$$$$$')
                for x in check_isra_result:
                    for id in data:
                        if str(x[0]) == id['grievace_id']:
                            id['action_to_be_taken_by'] = x[1]
                response_data['data'] = data
                return response_data

            if not exising_request_data:
                response_data = {
                    "status": 200,
                    "isRa": '',
                    "pranNo": pranNo,
                    "message": "No Record Found in the database",
                    "data": []
                }
                return response_data

        else:
            # add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request_pram,
            #             {"status": 403, "error": "Something went wrong. Please try again later."}, 'Failed')
            return {"status": 200,
                    "isRa": '',
                    "pranNo": pranNo,
                    "message": "No Record Found in the database or check you request parameters",
                    "data": []
                    }

    except (Exception, psycopg2.DatabaseError) as error:
        print("error==attendance request appled pending requestt==", error)
        # add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request_pram, error,
        #             'Failed')
        return {
            "status": 403,
            "isRa": '',
            "pranNo": pranNo,
            "message": str(error),
            "data": []
        }


@router.post("/applyGrievance")
def grivance_apply(pranNo: str = Body(..., Embed=True), grievace_type: int = Body(..., Embed=True),
                   grievace_subject: str = Body(..., Embed=True),
                   purpose: str = Body(..., Embed=True), attachment: str = Body(..., Embed=True),
                   file_extension: str = Body(..., Embed=True)):
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

        file_extension_list = ['.jpeg', '.png', '.pdf', '.mp3', '.mp4', '.jpg']
        if file_extension not in file_extension_list:
            response_data = {
                "status": 404,
                "message": "Only .jpeg, .jpg, .png, .pdf, .mp3, .mp4 format are allowed. Maximum file size is 2 MB",
            }
            return response_data

        # url = "http://localhost:9098/upload_images2"
        # url = "http://164.164.122.163:8087/upload_images2"
        url = base_url + "upload_images2"
        print(url, 'UUUUUUUUUUUU')

        payload = {
            "params": {
                "pranNo": pranNo,
                "grievace_type": grievace_type,
                "grievace_subject": grievace_subject,
                "purpose": purpose,
                "ufile": attachment,
            }
        }
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        res = response.json()
        print('response', res)
        if 'record_id' in res["result"]:
            record_id = res['result']['record_id']
            cur = engine.cursor()
            upd_face_query = cur.execute(
                f"update bsap_grievance set create_uid = '{employee_details[1]}',write_uid = '{employee_details[1]}' where id = '{record_id}'")
            engine.commit()
            cur.close()
            response_data = {
                "status": 200,
                "message": "Successfull",
                "obj_id": res['result']['record_id']
            }
            return response_data
        else:
            response_data = {
                "status": 403,
                "message": res,
            }
            return response_data

    except (Exception, psycopg2.DatabaseError) as error:
        return {
            "status": 403,
            "message": str(error)
        }


@router.get("/GrievanceType")
def grivance_type():
    '''
        provide all the type for grievance with id and name .
    '''
    try:
        engine = connectToDB()

        cur = engine.cursor()
        myquery = cur.execute(
            f"""select id, name from bsap_grievance_type;""")
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
                    'grievance_type_name': data[1]
                }
                data_list.append(data_dict)
            response = {
                "status": 200,
                "message": "Grievance Type List",
                "data": data_list
            }
        return response
    except (Exception, psycopg2.DatabaseError) as error:
        return {
            "status": 403,
            "message": str(error)
        }