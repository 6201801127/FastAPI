import time
import jwt
from decouple import config
from fastapi import Depends, HTTPException
from pydantic import ValidationError
from fastapi.security import HTTPBearer
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import urllib
import requests
import re

JWT_SECRET = config("secret")
JWT_ALGORITHM = config("algorithm")

reusable_oauth2 = HTTPBearer(scheme_name='Authorization')
employeeValidateId = {}


def token_response(token: str):
    return {
        "token": token
    }


def signJWT(mobileNo: str, userId: str, employeeId: str, otp_value: str):
    payload = {
        "mobileNo": mobileNo,
        "userId": userId,
        "employeeId": employeeId,
        "otp": otp_value,
        "expiry": f"{date.today() + relativedelta(years=+1)}"
    }
    # print("payload====", payload)
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return token_response(token)


def validate_token(http_authorization_credentials=Depends(reusable_oauth2)) -> str:
    # print("reusable_oauth2============", reusable_oauth2)
    # print("http_authorization_credentials============", http_authorization_credentials)
    # print("JWT_ALGORITHM-----------", JWT_ALGORITHM)
    # print("JWT_SECRET-----------", JWT_SECRET)
    try:
        decode_token = jwt.decode(http_authorization_credentials.credentials, JWT_SECRET, algorithms=JWT_ALGORITHM)
        # print("decode_token====",decode_token)
        # print("decode_token====",decode_token.get('expiry'))
        # print("emp data====",decode_token.get('employeeId'))
        if decode_token:
            # print("decode_token===", decode_token)
            # print("decode_token.get('expiry')", decode_token.get('expiry'), date.today())
            if datetime.date(datetime.strptime(decode_token.get('expiry'), '%Y-%m-%d')) < date.today():
                raise HTTPException(status_code=403, detail="Token expired")
            else:
                global employeeValidateId
                employeeValidateId['employeeId'] = decode_token.get('employeeId')
                employeeValidateId['expiry'] = decode_token.get('expiry')
                employeeValidateId['otp'] = decode_token.get('otp')
                employeeValidateId['userId'] = decode_token.get('userId')
                return True
    except(jwt.PyJWTError, ValidationError):
        raise HTTPException(
            status_code=403,
            detail=f"Could not validate credentials",
        )


def return_employee_data():
    try:
        decode_token = employeeValidateId
        if decode_token:
            return decode_token
    except:
        raise HTTPException(status_code=403, detail="Employee not found")


def decodeJWT(token: str):
    # print("called")
    try:
        # print("try",JWT_SECRET,JWT_ALGORITHM,token)
        decode_token = jwt.decode(token, JWT_SECRET, algorithm=JWT_ALGORITHM)
        # print("-------3333333-----",decode_token)
        return decode_token if decode_token['expires'] >= time.time() else None
    except:
        return {}
