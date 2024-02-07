from fastapi import Depends, FastAPI, HTTPException, Request, Body, Depends, Request
import uvicorn, json
from routers import grievance, login_user, leaves, attendance, eorderly, bsap_meeting_details

ATD_STATUS_PRESENT_LE, ATD_STATUS_PRESENT, ATD_STATUS_ABSENT, ATD_STATUS_LEAVE, ATD_STATUS_FHALF_LEAVE, ATD_STATUS_SHALF_LEAVE = '0', '1', '2', '3', '4', '5'
DAY_STATUS_WORKING, DAY_STATUS_LEAVE, DAY_STATUS_HOLIDAY, DAY_STATUS_RWORKING, DAY_STATUS_RHOLIDAY, DAY_STATUS_WEEKOFF = '0', '1', '2', '3', '4', '5'

app = FastAPI()
#
app.include_router(grievance.router)
app.include_router(login_user.router)
app.include_router(leaves.router)
app.include_router(attendance.router)
app.include_router(eorderly.router)
app.include_router(bsap_meeting_details.router)



if __name__ == '__main__':
    uvicorn.run("main:app", port=8080, host='0.0.0.0', reload=True, debug=True, workers=2)

