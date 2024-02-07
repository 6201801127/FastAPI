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

ATD_STATUS_PRESENT_LE, ATD_STATUS_PRESENT, ATD_STATUS_ABSENT, ATD_STATUS_LEAVE, ATD_STATUS_FHALF_LEAVE, ATD_STATUS_SHALF_LEAVE = '0', '1', '2', '3', '4', '5'
DAY_STATUS_WORKING, DAY_STATUS_LEAVE, DAY_STATUS_HOLIDAY, DAY_STATUS_RWORKING, DAY_STATUS_RHOLIDAY, DAY_STATUS_WEEKOFF = '0', '1', '2', '3', '4', '5'

router = APIRouter()


@router.post('/getMonthlyAttendanceStats/', dependencies=[Depends(validate_token)])
async def get_monthly_attendance_stats(request: dict = Body(...)):
    try:
        engine = connectToDB()

        pranNo = request.get('pranNo')
        month = request.get('month')
        year = request.get('year')
        if engine and pranNo and month and year:
            cur = engine.cursor()
            # logic
            response_data = {
                'status': 200
            }
            emp_data = cur.execute(f"select id,user_id,name from hr_employee where gpf_no = '{pranNo}';")
            emp = cur.fetchall()
            if not emp:
                emp_data = cur.execute(f"select id,user_id,name from hr_employee where employee_login_id = '{pranNo}';")
                emp = cur.fetchall()
            # print("No", emp, emp[0][0], year, month, datetime.now(), str(month.lstrip('0')))
            leave_data = cur.execute(f"""select to_date(to_char(date_from, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
            to_date(to_char(date_to, 'YYYY-MM-DD'), 'YYYY-MM-DD') from hr_leave 
            where employee_id = 1 and to_char(date_trunc('month', date_from),'MM') = '{str(month)}' 
            and to_char(date_trunc('year', date_from),'YYYY') = '{str(year)}';""")
            leaves = cur.fetchall()
            present_attn_data = cur.execute(f"""select atten_date from hr_attendance 
            where employee_id = {emp[0][0]} and year = '{str(year)}' and month = '{str(month.lstrip('0'))}' and state in ('0','1'); """)
            present_data = cur.fetchall()
            # print("present_data=", present_data)
            absent_attn_data = cur.execute(f"""select atten_date from hr_attendance 
            where employee_id = {emp[0][0]} and year = '{str(year)}' and month = '{str(month.lstrip('0'))}' and state='2'; """)
            absent_data = cur.fetchall()
            # print("absent_data=", absent_data)
            late_data = cur.execute(f""" select atten_date from hr_attendance 
            where employee_id = 1 and year = '{str(year)}' and month = '{str(month.lstrip('0'))}' and state ='0'; """)
            lates = cur.fetchall()
            # print('lates', lates)

            """ 1) Fetch all week-off defined in resource_calendar_leaves for the employee's shift if not in roster else fetch roster week offs
            """
            roster_data = cur.execute(f"""select date from bsap_employee_roaster_shift 
            where employee_id = '{emp[0][0]}' and to_char(date_trunc('month', date),'MM') =  '{str(month)}' 
            and to_char(date_trunc('year', date),'YYYY') = '{str(year)}' and week_off_status = True;""")
            roster = cur.fetchall()
            # print('roster', roster)
            week_offs = False
            if roster:
                week_offs = roster
            else:
                weekoffs_data = cur.execute(f""" select l.start_date from hr_employee a 
                join resource_calendar b on a.calendar_id = b.id 
                join resource_calendar_leaves l on b.id = l.calendar_id 
                where a.id =  {emp[0][0]} and to_char(date_trunc('month', l.start_date),'MM') = '{str(month)}'
                and to_char(date_trunc('year', l.start_date),'YYYY') = '{str(year)}' """)
                week_offs = cur.fetchall()

            """ Fetch the holidays
                1) Holidays is fixes in shifts for all shift types
            """
            holidays_data = cur.execute(f""" select date from hr_holidays_public_line 
            where to_char(date_trunc('month', date),'MM') = '{str(month)}'and to_char(date_trunc('year', date),'YYYY') = '{str(year)}' """)
            holidays = cur.fetchall()
            # print("holidays==", holidays)
            data = {}

            data['present'] = [present[0] for present in present_data]
            data['absent'] = [absent[0] for absent in absent_data]
            leave_dt = []
            if leaves:
                for leave in leaves:
                    from_dt, to_dt = leave[0], leave[1]
                    # print("from_dt,to_dt", from_dt, to_dt)
                    leave_dt += [datetime.strftime(from_dt + timedelta(days=x), '%d-%m-%Y') for x in
                                 range((to_dt - from_dt).days)] + [datetime.strftime(to_dt, '%d-%m-%Y')]
            data['leaves'] = leave_dt
            data['lates'] = [datetime.strftime(late_entry[0], '%d-%m-%Y') for late_entry in lates]
            data['weekoffs'] = [datetime.strftime(day[0], '%d-%m-%Y') for day in week_offs]
            data['holidays'] = [datetime.strftime(holiday[0], '%d-%m-%Y') for holiday in holidays]

            # print('data', data)
            cur.close()
            if data:
                cur.close()
                response_data['message'] = "Success."
                response_data['data'] = data
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request,
                            response_data, 'Success')
                return response_data
            else:
                cur.close()
                response_data['message'] = "No pending request found."
                response_data['data'] = []
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request,
                            response_data, 'Success')
                return response_data

    except (Exception, psycopg2.DatabaseError) as error:
        # print("error==ererererrrrrrrrrrrrrrr==", error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, error, 'Failed')
        return error


@router.post('/applyAttendance/')
async def apply_attendance(request: dict = Body(...)):
    try:
        """ UTC Conversion """
        engine = connectToDB()

        pranNo = request.get('pranNo')
        checkIn = datetime.strptime(str(request.get('checkIn')), '%d-%m-%Y %H:%M:%S:%f').astimezone(
            pytz.timezone('UTC')).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f")
        checkOut = datetime.strptime(str(request.get('checkOut')), '%d-%m-%Y %H:%M:%S:%f').astimezone(
            pytz.timezone('UTC')).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f")
        applyDate = datetime.strptime(str(request.get('applyDate')), '%d-%m-%Y %H:%M:%S:%f').astimezone(
            pytz.timezone('UTC')).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f")
        attendanceDate = datetime.strptime(str(request.get('attendanceDate')), '%d-%m-%Y').strftime("%Y-%m-%d")
        reason = request.get('reason')
        temp = reason.replace("'", r"")
        if engine and pranNo and checkIn and checkOut and applyDate and attendanceDate and reason:
            cur = engine.cursor()
            # logic
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
                "message": "Attendance Request Submitted"
            }
            exising_request = cur.execute(f"""select id,check_in_datetime,check_out_datetime from bsap_employee_apply_attendance 
                where employee_id = '{emp[0][0]}' and attendance_date = '{str(attendanceDate)}' and state not in ('1','3','4','5','6'); """)
            exising_request_data = cur.fetchall()
            if exising_request_data:
                upd_face_query = cur.execute(
                    f"""update bsap_employee_apply_attendance set check_in_datetime = '{checkIn}',
                check_out_datetime = '{checkOut}',remark = {str(temp)}, create_date = '{applyDate}', write_date = '{applyDate}' 
                where id = '{exising_request_data[0][0]}' and employee_id = '{emp[0][0]}' and state = '2'""")
                engine.commit()
                cur.close()
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, data,
                            'Success')
                return data
            else:
                add_request = cur.execute(f"""insert into bsap_employee_apply_attendance (applied_for,employee_id,branch_id,
                state,attendance_date,check_in_datetime,check_out_datetime,remark,create_date,write_date,create_uid,write_uid) 
                values('self','{emp[0][0]}','{emp[0][2]}','2','{attendanceDate}','{checkIn}','{checkOut}','{temp}','{applyDate}',
                '{applyDate}','{emp[0][1]}','{emp[0][1]}')""")
                # print("add_request=", add_request)
                engine.commit()
                cur.close()
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, data,
                            'Success')
                return data
        else:
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request,
                        {"status": 403, "error": "Something went wrong. Please try again later."}, 'Failed')
            return {"status": 403, "error": "Something went wrong. Please try again later."}
    except (Exception, psycopg2.DatabaseError) as error:
        # print("error==attendance apply request==", error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, error, 'Failed')
        return error


@router.post('/pendingAttendanceRequests/', dependencies=[Depends(validate_token)])
async def pending_attendance_requests():
    try:
        engine = connectToDB()

        if engine:
            cur = engine.cursor()
            # logic
            """ 1)Fetch all Pending attendance of the current authorised user
            """
            response_data = {
                'status': 200
            }
            data = []
            employee_id = return_employee_data().get('employeeId')
            ra_subs_data = cur.execute(f"""select id from hr_employee where parent_id = '{employee_id}'; """)
            ra_subs = [rec[0] for rec in cur.fetchall()]
            exising_request = cur.execute(f"""select id,employee_id,check_in_datetime,check_out_datetime,attendance_date,
            create_date,remark from bsap_employee_apply_attendance where employee_id in {tuple(ra_subs)} and state = '2'; """)
            exising_request_data = cur.fetchall()
            # print("exising_request_data==========)))))))))))))))))))))))))))))",exising_request_data)
            if exising_request_data:
                for record in exising_request_data:
                    # print("---------------------------------------------jj",pytz.timezone('UTC').localize(record[2]).astimezone(pytz.timezone('Asia/Kolkata')).replace(tzinfo=None).strftime("%d-%m-%Y %H:%M:%S"),pytz.timezone('UTC').localize(record[3]).astimezone(pytz.timezone('Asia/Kolkata')).replace(tzinfo=None).strftime("%d-%m-%Y %H:%M:%S"))
                    emp_data = cur.execute(
                        f"select e.name,e.gpf_no,j.name from hr_employee e join hr_job j on e.job_id = j.id  where e.id = '{record[1]}';")
                    emp = cur.fetchall()
                    # print("emp=",emp)
                    base_url = f"{config('base_url', default='http://192.168.61.86:8069/')}web/image?model=hr.employee&field=image_1920&id={record[1]}&unique="
                    request_data = {
                        "id": record[0],
                        "empName": emp[0][0],
                        "empPranNo": emp[0][1],
                        "profilePic": base_url if base_url else "",
                        "designation": emp[0][2],
                        "applicationDate": datetime.strptime(str(record[5]), '%Y-%m-%d %H:%M:%S.%f').strftime(
                            "%d-%m-%Y") if record[5] else "",
                        "attendanceDate": datetime.strptime(str(record[4]), '%Y-%m-%d').strftime("%d-%m-%Y") if record[
                            4] else "",
                        "checkIn": pytz.timezone('UTC').localize(record[2]).astimezone(
                            pytz.timezone('Asia/Kolkata')).replace(tzinfo=None).strftime("%d-%m-%Y %H:%M:%S") if record[
                            2] else "",
                        "checkOut": pytz.timezone('UTC').localize(record[3]).astimezone(
                            pytz.timezone('Asia/Kolkata')).replace(tzinfo=None).strftime("%d-%m-%Y %H:%M:%S") if record[
                            3] else "",
                        "remark": record[6],
                    }
                    data.append(request_data)
                cur.close()
                # print("data=", data)
                response_data['message'] = "Success"
                response_data['data'] = data
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {},
                            response_data, 'Success')
                return response_data
            else:
                cur.close()
                response_data['message'] = "No pending request found."
                response_data['data'] = []
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {},
                            response_data, 'Success')
                return response_data
        else:
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {},
                        {"status": 403, "error": "Something went wrong. Please try again later."}, 'Failed')
            return {"status": 403, "error": "Something went wrong. Please try again later."}
    except (Exception, psycopg2.DatabaseError) as error:
        # print("error==attendance pending request==", error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {}, error, 'Failed')
        return error


@router.post('/approveAttendanceRequest/')
async def approve_attendance_request(request: dict = Body(...)):
    try:
        engine = connectToDB()
        status = request.get('status')
        requestId = request.get('requestId')
        remark = request.get('remark')
        raId = request.get('raId')
        # print(" status= ", status, " requestId= ", requestId, " remark= ", remark, " raId= ", raId)
        if engine and requestId and remark and raId:
            cur = engine.cursor()
            # logic
            """ 1)Approved the request of the give id,
                2) Update/Create the attendance
                3) status actions are as follows:-
                0:- Reject
                1:- Approve
                2:- Forward (Optional)
            """
            state = False
            data = {"status": 200}
            if status == 0:
                # print("rejected request")
                state = 5
                data["message"] = "Request Rejected."
            if status == 1:
                # print("Approved request")
                state = 3
                data["message"] = "Request Approved."
                """ Get employee data """
                emp_details = cur.execute(f""" select employee_id,attendance_date,check_in_datetime,check_out_datetime 
                from bsap_employee_apply_attendance where id = '{requestId}'; """)
                emp_data = cur.fetchall()
                """ Finding Employee Day status """
                attendance_status = ATD_STATUS_PRESENT if emp_data[0][2] else ATD_STATUS_ABSENT
                # #if its a working day
                """ Leave status """
                emp_leave = cur.execute(f""" SELECT id,request_unit_half,request_unit_half_2 FROM hr_leave 
                WHERE employee_id = '{emp_data[0][0]}' and request_date_from >= '{emp_data[0][1]}' and request_date_to <= '{emp_data[0][1]}'""")
                leave_data = cur.fetchall()
                if emp_leave:
                    if emp_leave[0][1]:
                        attendance_status = ATD_STATUS_FHALF_LEAVE
                    elif emp_leave[0][2]:
                        attendance_status = ATD_STATUS_SHALF_LEAVE
                    else:
                        attendance_status = ATD_STATUS_LEAVE
                # return the shift details of employee (Normal/Roaster)
                shift_data = False
                roster_data = cur.execute(f""" SELECT a.shift_id,b.name,b.id,b.grace_time,b.first_half_hour_from,b.second_half_hour_from,
                a.week_off_status FROM bsap_employee_roaster_shift a join resource_calendar b on a.shift_id = b.id 
                WHERE a.employee_id = '{emp_data[0][0]}' and a.date = '{emp_data[0][1]}' LIMIT 1 """)
                roaster_dt = cur.fetchall()
                if roaster_dt:
                    shift_data = roaster_dt
                else:
                    employee_shift_data = cur.execute(f""" SELECT a.calendar_id,b.name,b.id,b.grace_time,b.first_half_hour_from,
                    b.second_half_hour_from from hr_employee a join resource_calendar b on a.calendar_id = b.id 
                    WHERE a.id = '{emp_data[0][0]}' LIMIT 1 """)
                    shift_data = cur.fetchall()
                """ Absent status """
                if not emp_data[0][3] and not emp_data[0][2] and attendance_status != ATD_STATUS_LEAVE:
                    attendance_status = ATD_STATUS_ABSENT
                """ Present Late Entry status """
                if emp_data[0][2] and shift_data[0][3] and attendance_status != ATD_STATUS_LEAVE:
                    if emp_data[0][1]:
                        fhalf_in_time = datetime.strptime((str(emp_data[0][1]) + ' ' + str(
                            int(shift_data[0][4])) + ':' + str(int(shift_data[0][3] * 60)) + ':00'),
                                                          "%Y-%m-%d %H:%M:%S")
                        shalf_out_time = datetime.strptime((str(emp_data[0][1]) + ' ' + str(
                            int(shift_data[0][5])) + ':' + str(int(shift_data[0][3] * 60)) + ':00'),
                                                           "%Y-%m-%d %H:%M:%S")
                        intime_data = datetime.strptime(str(emp_data[0][2]), '%Y-%m-%d %H:%M:%S.%f')
                        if fhalf_in_time < intime_data < shalf_out_time:
                            attendance_status = ATD_STATUS_PRESENT_LE
                        if intime_data > shalf_out_time:
                            attendance_status = ATD_STATUS_PRESENT_LE
                """ Finding Employee Day status """
                day_status = DAY_STATUS_WORKING
                if roaster_dt:
                    day_status = DAY_STATUS_RHOLIDAY if roaster_dt[0][6] else DAY_STATUS_RWORKING
                else:
                    holidays_data = cur.execute(f""" select e.name,a.branch_id,b.date from hr_holiday_public_branch_rel a 
                    join hr_holidays_public_line b on a.line_id = b.id 
                    join hr_employee e on e.current_office_id = a.branch_id and e.id = '{emp_data[0][0]}' and b.date ='{emp_data[0][1]}' """)
                    holidays = cur.fetchall()
                    if holidays:
                        day_status = DAY_STATUS_HOLIDAY
                    else:
                        week_off_data = cur.execute(f""" select a.start_date from resource_calendar_leaves a 
                        join hr_employee b on a.calendar_id = b.current_office_id 
                        where a.start_date = '{emp_data[0][1]}' and a.holiday_type ='1' and b.id = '{emp_data[0][0]}'""")
                        week_off = cur.fetchall()
                        if week_off:
                            day_status = DAY_STATUS_WEEKOFF
                # print("day_status", day_status, attendance_status)
                exising_request = cur.execute(f"""select id,employee_id,check_in_datetime,check_out_datetime,attendance_date,
                remark from bsap_employee_apply_attendance where id = '{requestId}'; """)
                exising_request_data = cur.fetchall()
                # print("exising_request_data===", exising_request_data)
                update_atten_Data = cur.execute(f"""update hr_attendance set check_in='{exising_request_data[0][2]}',
                check_out='{exising_request_data[0][3]}', state='{attendance_status}',day_status='{day_status}' 
                where atten_date = '{exising_request_data[0][4]}' and employee_id = '{exising_request_data[0][1]}'; """)
                engine.commit()
            if status == 2:
                # print("Forwarded request")
                pass
                data["message"] = "Request Forwarded."
            exising_request = cur.execute(
                f"""update bsap_employee_apply_attendance set state='{state}',authority_remark='{remark}',
            action_taken_by='{raId}',action_taken_on='{datetime.now()}' where id = '{requestId}'; """)
            engine.commit()
            cur.close()
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, data,
                        'Success')
            return data
        else:
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request,
                        {"status": 403, "error": "Something went wrong. Please try again later."}, 'Success')
            return {"status": 403, "error": "Something went wrong. Please try again later."}
    except (Exception, psycopg2.DatabaseError) as error:
        # print("error==attendance Approve request==", error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, error, 'Failed')
        return error


@router.post('/pendingOffSiteRequests/', dependencies=[Depends(validate_token)])
async def pending_off_site_requests():
    try:
        engine = connectToDB()
        if engine:
            cur = engine.cursor()
            # logic
            """ 1)Fetch all Pending attendance of the current authorised user
            """
            response_data = {
                'status': 200
            }
            data = []
            employee_id = return_employee_data().get('employeeId')
            ra_subs_data = cur.execute(f"""select id from hr_employee where parent_id = '{employee_id}'; """)
            ra_subs = [rec[0] for rec in cur.fetchall()]
            # print("ra_subs",ra_subs)
            exising_request = cur.execute(f"""select id,employee_id,check_in_datetime,check_out_datetime,attendance_date,create_date 
            from bsap_employee_apply_attendance 
            where employee_id in {tuple(ra_subs)} and state = '2' and offsite = 'True'; """)
            exising_request_data = cur.fetchall()
            # print("exising_request_data",exising_request_data)
            if exising_request_data:
                for record in exising_request_data:
                    emp_data = cur.execute(
                        f"select e.name,e.gpf_no,j.name from hr_employee e join hr_job j on e.job_id = j.id  where e.id = '{record[1]}';")
                    emp = cur.fetchall()
                    atten_data = cur.execute(
                        f"select lat,lng,cout_lat,cout_lng,location,remark,cout_remark from hr_attendance where atten_date = '{record[4]}';")
                    atten = cur.fetchall()
                    base_url = f"{config('base_url', default='http://192.168.61.86:8069/')}web/image?model=hr.employee&field=image_1920&id={record[1]}&unique="
                    request_data = {
                        "id": record[0],
                        "empName": emp[0][0],
                        "empPranNo": emp[0][1],
                        "profilePic": base_url if base_url else "",
                        "designation": emp[0][2],
                        "applicationDate": datetime.strptime(str(record[5]), '%Y-%m-%d %H:%M:%S.%f').strftime(
                            "%d-%m-%Y") if record[5] else "",
                        "attendanceDate": datetime.strptime(str(record[4]), '%Y-%m-%d').strftime("%d-%m-%Y") if record[
                            4] else "",
                        "checkIn": pytz.timezone('UTC').localize(record[2]).astimezone(
                            pytz.timezone('Asia/Kolkata')).replace(tzinfo=None).strftime("%d-%m-%Y %H:%M:%S") if record[
                            2] else "",
                        "checkOut": pytz.timezone('UTC').localize(record[3]).astimezone(
                            pytz.timezone('Asia/Kolkata')).replace(tzinfo=None).strftime("%d-%m-%Y %H:%M:%S") if record[
                            3] else "",
                        "checkin_lat": atten[0][0] if atten[0][0] else '',
                        "checkin_lng": atten[0][1] if atten[0][1] else '',
                        "checkout_lng": atten[0][3] if atten[0][3] else '',
                        "checkout_lat": atten[0][2] if atten[0][2] else '',
                        "location:": atten[0][4] if atten[0][4] else '',
                        "checkin_remark": atten[0][5] if atten[0][5] else '',
                        "checkout_remark": atten[0][6] if atten[0][6] else '',
                    }
                    data.append(request_data)
                cur.close()
                response_data['message'] = "Success"
                response_data['data'] = data
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {},
                            response_data, 'Success')
                return response_data
            else:
                cur.close()
                response_data['message'] = "No pending request found."
                response_data['data'] = []
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {},
                            response_data, 'Success')
                return response_data
        else:
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {},
                        {"status": 403, "error": "Something went wrong. Please try again later."}, 'Failed')
            return {"status": 403, "error": "Something went wrong. Please try again later."}
    except (Exception, psycopg2.DatabaseError) as error:
        # print("error==attendance pending request==", error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), {}, error, 'Failed')
        return error


@router.post('/approveOffSiteAttedance/')
async def approve_off_site_attedance(request: dict = Body(...)):
    try:
        engine = connectToDB()
        status = request.get('status')
        requestId = request.get('requestId')
        remark = request.get('remark')
        raId = request.get('raId')
        # print("status= ", status, " requestId= ", requestId, " remark= ", remark, " raId= ", raId)
        if engine and requestId and remark and raId:
            cur = engine.cursor()
            # logic
            """ 1)Approved the request of the give id,
                2) Update the attendance
                3) status actions are as follows:-
                0:- Reject
                1:- Approve
                2:- Forward (Optional)
            """
            state = False
            data = {"status": 200}
            if status == 0:
                # print("rejected request")
                state = 5
                data["message"] = "Request Rejected."
            if status == 1:
                # print("Approved request")
                state = 3
                data["message"] = "Request Approved."
                """ Get employee data """
                emp_details = cur.execute(f""" select employee_id,attendance_date,check_in_datetime,check_out_datetime 
                from bsap_employee_apply_attendance where id = '{requestId}'; """)
                emp_data = cur.fetchall()
                """ Finding Employee Day status """
                attendance_status = ATD_STATUS_PRESENT if emp_data[0][2] else ATD_STATUS_ABSENT
                # #if its a working day
                """ Leave status """
                emp_leave = cur.execute(f""" SELECT id,request_unit_half,request_unit_half_2 FROM hr_leave 
                WHERE employee_id = '{emp_data[0][0]}' and request_date_from >= '{emp_data[0][1]}' and request_date_to <= '{emp_data[0][1]}'""")
                leave_data = cur.fetchall()
                if emp_leave:
                    if emp_leave[0][1]:
                        attendance_status = ATD_STATUS_FHALF_LEAVE
                    elif emp_leave[0][2]:
                        attendance_status = ATD_STATUS_SHALF_LEAVE
                    else:
                        attendance_status = ATD_STATUS_LEAVE
                # return the shift details of employee (Normal/Roaster)
                shift_data = False
                roster_data = cur.execute(f""" SELECT a.shift_id,b.name,b.id,b.grace_time,b.first_half_hour_from,b.second_half_hour_from,a.week_off_status 
                FROM bsap_employee_roaster_shift a 
                join resource_calendar b on a.shift_id = b.id 
                WHERE a.employee_id = '{emp_data[0][0]}' and a.date = '{emp_data[0][1]}' LIMIT 1 """)
                roaster_dt = cur.fetchall()
                if roaster_dt:
                    shift_data = roaster_dt
                else:
                    employee_shift_data = cur.execute(f""" SELECT a.calendar_id,b.name,b.id,b.grace_time,b.first_half_hour_from,b.second_half_hour_from 
                    from hr_employee a 
                    join resource_calendar b on a.calendar_id = b.id 
                    WHERE a.id = '{emp_data[0][0]}' LIMIT 1 """)
                    shift_data = cur.fetchall()
                """ Absent status """
                if not emp_data[0][3] and not emp_data[0][2] and attendance_status != ATD_STATUS_LEAVE:
                    attendance_status = ATD_STATUS_ABSENT
                """ Present Late Entry status """
                if emp_data[0][2] and shift_data[0][3] and attendance_status != ATD_STATUS_LEAVE:
                    if emp_data[0][1]:
                        fhalf_in_time = datetime.strptime((str(emp_data[0][1]) + ' ' + str(
                            int(shift_data[0][4])) + ':' + str(int(shift_data[0][3] * 60)) + ':00'),
                                                          "%Y-%m-%d %H:%M:%S")
                        shalf_out_time = datetime.strptime((str(emp_data[0][1]) + ' ' + str(
                            int(shift_data[0][5])) + ':' + str(int(shift_data[0][3] * 60)) + ':00'),
                                                           "%Y-%m-%d %H:%M:%S")
                        intime_data = datetime.strptime(str(emp_data[0][2]), '%Y-%m-%d %H:%M:%S.%f')
                        if fhalf_in_time < intime_data < shalf_out_time:
                            attendance_status = ATD_STATUS_PRESENT_LE
                        if intime_data > shalf_out_time:
                            attendance_status = ATD_STATUS_PRESENT_LE
                """ Finding Employee Day status """
                day_status = DAY_STATUS_WORKING
                if roaster_dt:
                    day_status = DAY_STATUS_RHOLIDAY if roaster_dt[0][6] else DAY_STATUS_RWORKING
                else:
                    holidays_data = cur.execute(f""" select e.name,a.branch_id,b.date from hr_holiday_public_branch_rel a 
                    join hr_holidays_public_line b on a.line_id = b.id 
                    join hr_employee e on e.current_office_id = a.branch_id and e.id = '{emp_data[0][0]}' and b.date ='{emp_data[0][1]}' """)
                    holidays = cur.fetchall()
                    if holidays:
                        day_status = DAY_STATUS_HOLIDAY
                    else:
                        week_off_data = cur.execute(f""" select a.start_date from resource_calendar_leaves a 
                        join hr_employee b on a.calendar_id = b.current_office_id 
                        where a.start_date = '{emp_data[0][1]}' and a.holiday_type ='1' and b.id = '{emp_data[0][0]}' """)
                        week_off = cur.fetchall()
                        if week_off:
                            day_status = DAY_STATUS_WEEKOFF
                exising_request = cur.execute(f"""select id,employee_id,check_in_datetime,check_out_datetime,attendance_date,remark 
                from bsap_employee_apply_attendance where id = '{requestId}'; """)
                exising_request_data = cur.fetchall()
                update_atten_Data = cur.execute(f"""update hr_attendance set check_in='{exising_request_data[0][2]}',
                check_out='{exising_request_data[0][3]}', state='{attendance_status}',day_status='{day_status}' 
                where atten_date = '{exising_request_data[0][4]}' and employee_id = '{exising_request_data[0][1]}'; """)
                engine.commit()
            if status == 2:
                # print("Forwarded request")
                pass
                data["message"] = "Request Forwarded."
            exising_request = cur.execute(
                f"""update bsap_employee_apply_attendance set state='{state}',authority_remark='{remark}',
            action_taken_by='{raId}',action_taken_on='{datetime.now()}' 
            where id = '{requestId}'; """)
            engine.commit()
            cur.close()
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, data,
                        'Success')
            return data
        else:
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request,
                        {"status": 403, "error": "Something went wrong. Please try again later."}, 'Failed')
            return {"status": 403, "error": "Something went wrong. Please try again later."}
    except (Exception, psycopg2.DatabaseError) as error:
        # print("error==attendance Approve request==", error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, error, 'Failed')
        return error


@router.post('/getUserAppliedAttendance/')
async def get_user_applied_attendance(request: dict = Body(...)):
    try:
        engine = connectToDB()
        pranNo = request.get('pranNo')
        if engine and pranNo:
            cur = engine.cursor()
            # logic
            """ 1)Fetch all Pending attendance of the current authorised user
            """
            response_data = {
                'status': 200
            }
            data = []
            employee_data = cur.execute(f"""select id from hr_employee where gpf_no = '{pranNo}'; """)
            employee_id = cur.fetchall()
            if not employee_id:
                employee_data = cur.execute(f"""select id from hr_employee where employee_login_id = '{pranNo}'; """)
                employee_id = cur.fetchall()
            exising_request = cur.execute(f"""select id,employee_id,check_in_datetime,check_out_datetime,attendance_date,
            create_date,remark from bsap_employee_apply_attendance 
            where employee_id = {employee_id[0][0]} and state = '2' order by id desc; """)
            exising_request_data = cur.fetchall()
            # print("exising_request_data==========&&&&&",exising_request_data)
            if exising_request_data:
                for record in exising_request_data:
                    emp_data = cur.execute(
                        f"select e.name,e.gpf_no,j.name from hr_employee e join hr_job j on e.job_id = j.id  where e.id = '{record[1]}';")
                    emp = cur.fetchall()
                    # print("emp=",emp)
                    base_url = f"{config('base_url', default='http://192.168.61.86:8069/')}web/image?model=hr.employee&field=image_1920&id={record[1]}&unique="
                    request_data = {
                        "id": record[0],
                        "applicationDate": datetime.strptime(str(record[5]), '%Y-%m-%d %H:%M:%S.%f').strftime(
                            "%d-%m-%Y") if record[5] else "",
                        "applicationStatus": 3,
                        "attendanceDate": datetime.strptime(str(record[4]), '%Y-%m-%d').strftime("%d-%m-%Y") if record[
                            4] else "",
                        "checkIn": datetime.strptime(str(record[2]), '%Y-%m-%d %H:%M:%S.%f').strftime(
                            "%d-%m-%Y %H:%M:%S") if record[2] else "",
                        "checkOut": datetime.strptime(str(record[3]), '%Y-%m-%d %H:%M:%S.%f').strftime(
                            "%d-%m-%Y %H:%M:%S") if record[3] else "",
                        "remark": record[6],
                    }
                    data.append(request_data)
                cur.close()
                # print("data=", data)
                response_data['message'] = "Success"
                response_data['data'] = data
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request,
                            response_data, 'Success')
                return response_data
            else:
                cur.close()
                response_data['message'] = "No pending request found."
                response_data['data'] = []
                add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request,
                            response_data, 'Success')
                return response_data
        else:
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request,
                        {"status": 403, "error": "Something went wrong. Please try again later."}, 'Failed')
            return {"status": 403, "error": "Something went wrong. Please try again later."}
    except (Exception, psycopg2.DatabaseError) as error:
        # print("error==attendance request applied pending request==", error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, error, 'Failed')
        return error


@router.post('/updateAttendance/', dependencies=[Depends(validate_token)])
async def updateattendance(request: list = Body(...)):
    try:
    # a = True
    # if a:
        engine = connectToDB()
        if engine and request:
            cur = engine.cursor()
            for rec in request:
                pranNo, attendance = rec.get('pranNo'), rec.get('attendance')
                emp_query = cur.execute(
                    f"""SELECT id,user_id,current_office_id FROM hr_employee WHERE gpf_no = '{pranNo}'""")
                emp_data = cur.fetchall()
                if not emp_data:
                    emp_query = cur.execute(
                        f"""SELECT id,user_id,current_office_id FROM hr_employee WHERE employee_login_id = '{pranNo}'""")
                    emp_data = cur.fetchall()
                print(emp_data, 'emp_data')
                date_time = date.today()
                if emp_data and attendance:
                    for attn in attendance:
                        """ UTC conversion to store in Database """
                        attn_date = datetime.strptime(attn.get('date'), '%d-%m-%Y').strftime("%Y-%m-%d")
                        attn_month = datetime.strptime(attn.get('date'), '%d-%m-%Y').strftime("%m")
                        attn_year = datetime.strptime(attn.get('date'), '%d-%m-%Y').strftime("%Y")
                        print(type(attn_year), attn_year, type(attn_month), attn_month, 'APAPAPAPPPPPPPPPPP')
                        if attn.get('checkIn'):
                            in_time = datetime.strptime(str(attn.get('checkIn')), '%d-%m-%Y %H:%M:%S:%f').astimezone(
                                pytz.timezone('UTC')).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f")
                            in_time = "'"+in_time+"'"
                        if not attn.get('checkIn'):
                            in_time = 'Null'
                        print(attn.get('checkOut'), 'DDDDDDDDDDDDDD')
                        if attn.get('checkOut'):
                            out_time = datetime.strptime(str(attn.get('checkOut')), '%d-%m-%Y %H:%M:%S:%f').astimezone(
                                pytz.timezone('UTC')).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f")
                            out_time = "'"+out_time+"'"
                        if not attn.get('checkOut'):
                            out_time = 'Null'
                        """ Creating attendance correction request """
                        if attn.get('insideGeoFence') == True:
                            print("inside", 'PPPP')
                            add_request = cur.execute(f"""insert into bsap_employee_apply_attendance (applied_for,employee_id,branch_id,
                            state,attendance_date,check_in_datetime,check_out_datetime,remark,create_date,write_date,create_uid,
                            write_uid,offsite) 
                            values('self','{emp_data[0][0]}','{emp_data[0][2]}','2','{attn_date}',{in_time},{out_time},
                            '{attn.get('checkin_remark')}','{datetime.now()}','{datetime.now()}','{emp_data[0][1]}','{emp_data[0][1]}',
                            'True')""")
                            engine.commit()
                        check_existing_data = cur.execute(
                            f"""select check_in from hr_attendance where employee_id = '{emp_data[0][0]}' and atten_date = '{attn_date}'""")
                        existing_attn_data = cur.fetchall()
                        print(existing_attn_data, 'existing_attn_data')

                        """ Finding Employee Day status """
                        attendance_status = ATD_STATUS_PRESENT if in_time != 'Null' else ATD_STATUS_ABSENT
                        # #if its a working day
                        """ Leave status """
                        emp_leave = cur.execute(f""" SELECT id,request_unit_half,request_unit_half_2 FROM hr_leave 
                        WHERE employee_id = '{emp_data[0][0]}' and request_date_from >= '{attn_date}' and request_date_to <= '{attn_date}'""")
                        leave_data = cur.fetchall()
                        print(leave_data, 'leave_data')
                        if emp_leave:
                            if emp_leave[0][1]:
                                attendance_status = ATD_STATUS_FHALF_LEAVE
                            elif emp_leave[0][2]:
                                attendance_status = ATD_STATUS_SHALF_LEAVE
                            else:
                                attendance_status = ATD_STATUS_LEAVE
                        # return the shift details of employee (Normal/Roaster)
                        shift_data = False
                        roster_data = cur.execute(f""" SELECT a.shift_id,b.name,b.id,b.grace_time,b.first_half_hour_from,
                        b.second_half_hour_from,a.week_off_status 
                        FROM bsap_employee_roaster_shift a 
                        join resource_calendar b on a.shift_id = b.id 
                        WHERE a.employee_id = '{emp_data[0][0]}' and a.date = '{attn_date}' LIMIT 1 """)
                        roaster_dt = cur.fetchall()
                        print(roaster_dt, 'roater_dt')
                        if roaster_dt:
                            shift_data = roaster_dt
                        else:
                            employee_shift_data = cur.execute(f""" SELECT a.calendar_id,b.name,b.id,b.grace_time,
                            b.first_half_hour_from, b.second_half_hour_from 
                            from hr_employee a 
                            join resource_calendar b on a.calendar_id = b.id 
                            WHERE a.id = '{emp_data[0][0]}' LIMIT 1 """)
                            shift_data = cur.fetchall()
                            print(shift_data, 'shift_data')
                        """ Absent status """
                        if out_time == 'Null' and in_time == 'Null' and attendance_status != ATD_STATUS_LEAVE:
                            attendance_status = ATD_STATUS_ABSENT
                        """ Present Late Entry status """
                        if in_time != 'Null' and shift_data[0][3] and attendance_status != ATD_STATUS_LEAVE:
                            if attn_date:
                                fhalf_in_time = datetime.strptime((attn_date + ' ' + str(
                                    int(shift_data[0][4])) + ':' + str(int(shift_data[0][3] * 60)) + ':00'),
                                                                  "%Y-%m-%d %H:%M:%S")
                                shalf_out_time = datetime.strptime((attn_date + ' ' + str(
                                    int(shift_data[0][5])) + ':' + str(int(shift_data[0][3] * 60)) + ':00'),
                                                                   "%Y-%m-%d %H:%M:%S")
                                in_time_new = in_time[1:-1]
                                intime_data = datetime.strptime(in_time_new, '%Y-%m-%d %H:%M:%S.%f')
                                if fhalf_in_time < intime_data < shalf_out_time:
                                    attendance_status = ATD_STATUS_PRESENT_LE
                                if intime_data > shalf_out_time:
                                    attendance_status = ATD_STATUS_PRESENT_LE
                        """ Finding Employee Day status """
                        day_status = DAY_STATUS_WORKING
                        if roaster_dt:
                            day_status = DAY_STATUS_RHOLIDAY if roaster_dt[0][6] else DAY_STATUS_RWORKING
                        else:
                            holidays_data = cur.execute(f""" select e.name,a.branch_id,b.date from hr_holiday_public_branch_rel a 
                            join hr_holidays_public_line b on a.line_id = b.id 
                            join hr_employee e on e.current_office_id = a.branch_id and e.id = '{emp_data[0][0]}' and b.date ='{attn_date}' """)
                            holidays = cur.fetchall()
                            print(holidays, 'holidays')
                            if holidays:
                                day_status = DAY_STATUS_HOLIDAY
                            else:
                                week_off_data = cur.execute(f""" select a.start_date from resource_calendar_leaves a 
                                join hr_employee b on a.calendar_id = b.current_office_id 
                                where a.start_date = '{attn_date}' and a.holiday_type ='1' and b.id = '{emp_data[0][0]}' """)
                                week_off = cur.fetchall()
                                print(week_off, 'weekoff')
                                if week_off:
                                    day_status = DAY_STATUS_WEEKOFF
                        if existing_attn_data:
                            # update
                            attendance_update_query = cur.execute(f"""update hr_attendance set check_out = {out_time},
                            check_in = {in_time},inside_geo_fence = '{attn.get('insideGeoFence')}',
                            remark = '{attn.get('remark')}',lat = '{attn.get('lat')}',lng = '{attn.get('lng')}',
                            location = '{attn.get('location')}',cout_remark = '{attn.get('checkout_remark')}',
                            cout_lat = '{attn.get('checkout_lat')}',cout_lng = '{attn.get('checkout_lng')}',
                            state='{attendance_status}', day_status='{day_status}' , month='{attn_month}', year='{attn_year}'
                            where employee_id ='{emp_data[0][0]}' and atten_date = '{attn_date}'""")
                        else:
                            # insert new attendance data
                            attendance_create_query = cur.execute(f"""insert into hr_attendance (employee_id,atten_date,
                            check_in,check_out,inside_geo_fence,remark,lat,lng,location,create_uid,write_uid,create_date,
                            write_date,cout_remark,cout_lat,cout_lng,state,day_status, month, year) 
                            values('{emp_data[0][0]}','{attn_date}',{in_time},{out_time},'{attn.get('insideGeoFence')}',
                            '{attn.get('checkin_remark')}','{attn.get('checkin_lat')}','{attn.get('checkin_lng')}',
                            '{attn.get('location')}','{emp_data[0][1]}','{emp_data[0][1]}','{datetime.now()}',
                            '{datetime.now()}','{attn.get('checkout_remark')}','{attn.get('checkout_lat')}',
                            '{attn.get('checkout_lng')}','{attendance_status}','{day_status}', '{attn_month}', '{attn_year}')""")
                        # attendance log
                        attendance_log_query = cur.execute(f"""insert into bsap_bio_atd_enroll_info (user_id,date,
                        checkin,checkout,lat,lng,location,inside_geo_fence,remark,create_uid,write_uid,create_date,
                        write_date,cout_remark,cout_lat,cout_lng) 
                        values('{emp_data[0][0]}','{attn_date}',{in_time},{out_time},'{attn.get('lat')}','{attn.get('lng')}',
                        '{attn.get('location')}','{attn.get('insideGeoFence')}','{attn.get('remark')}','{emp_data[0][1]}',
                        '{emp_data[0][1]}','{datetime.now()}','{datetime.now()}','{attn.get('checkout_remark')}',
                        '{attn.get('checkout_lat')}','{attn.get('checkout_lng')}')""")
                        engine.commit()
            cur.close()
            return_data = {"status": 200, "message": "Success"}
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, return_data,
                        'Success')
            return return_data
        else:
            return_data = {"status": 403, "error": "Something went wrong. Please try again later."}
            add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, return_data,
                        'Failed')
            return return_data

    except (Exception, psycopg2.DatabaseError) as error:
        # print("error==11111111111111111111111==", error)
        add_api_log(datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"), request, error, 'Failed')
        return error