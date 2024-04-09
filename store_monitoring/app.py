from flask import Flask, request, send_from_directory
from flask_cors import CORS 
import uuid
import sqlite3 as sql
import asyncio
from datetime import datetime, timedelta, timezone
import pytz
import csv  

app = Flask(__name__)
CORS(app)
timestamp_fmt = '%Y-%m-%d %H:%M:%S.%f %Z' #2023-01-24 09:08:23.138922 UTC
bussinesshours_fmt = '%H:%M:%S'
sortableTimestamp_fmt = '%Y%m%d%H%M%S'
time_fmt = '%H%M%S'

# start generating the report
@app.get("/trigger_report")
def trigger_report():
    report_id = uuid.uuid4()
    # insert into db 
    with sql.connect("StoreDatabase.db") as con:
        cur = con.cursor()
        cur.execute("INSERT INTO ReportStatus (reportId,status) VALUES (?,?)",(str(report_id), "generating"))
        con.commit()
    loop = get_set_event_loop()
    loop.create_task(generate_report(report_id))
    return str(report_id)


def get_dayOfWeek(timestamp):
    datetime_obj = datetime.strptime(timestamp, timestamp_fmt)
    return datetime_obj.weekday()

def getTime(timestamp):
    datetime_obj = datetime.strptime(timestamp, timestamp_fmt)
    return int(datetime_obj.strftime(time_fmt))

def getSortableTime(timestamp):
    datetime_obj = datetime.strptime(timestamp, timestamp_fmt)
    return datetime_obj.strftime(sortableTimestamp_fmt)

async def generate_report(report_id):
    report_fields = ['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week', 'downtime_last_hour', 'downtime_last_day', 'downtime_last_week']  
    report_rows = []
    print("loop starting")
    with sql.connect("StoreDatabase.db") as con:
        con.row_factory = sql.Row
        con.create_function('getDayOfWeek',1,get_dayOfWeek)
        con.create_function('getTime',1,getTime)
        con.create_function('getSortableTime',1,getSortableTime)
        cur = con.cursor()
        cur.execute("select * from StoreTimezones")
        stores = cur.fetchall(); 
        for store in stores:
            try:
                #  get store times and convert to UTC
                storeTimes = cur.execute("select * from StoreHours WHERE store_id = {}".format(store['store_id'])).fetchall()
                startTimePerDay = [None,None,None,None,None,None,None]
                endTimePerDay = [None,None,None,None,None,None,None]
                store_timezone = store['timezone_str'] if store['timezone_str'] != '' else 'America/Chicago'
                for day in storeTimes:
                    startTime_local_str = datetime.strptime(day['start_time_local'], bussinesshours_fmt)
                    startTimelocal = pytz.timezone(store_timezone).localize(startTime_local_str, is_dst=None)
                    endTime_local_str = datetime.strptime(day['end_time_local'], bussinesshours_fmt)
                    endTimelocal = pytz.timezone(store_timezone).localize(endTime_local_str, is_dst=None)
                    startTime = int(startTimelocal.astimezone(timezone.utc).strftime(time_fmt))
                    endTime = int(endTimelocal.astimezone(timezone.utc).strftime(time_fmt))
                    if startTime < endTime:
                        startTimePerDay[int(day['day'])] = startTime
                        endTimePerDay[int(day['day'])] = endTime
                    else: 
                        startTimePerDay[int(day['day'])] = endTime
                        endTimePerDay[int(day['day'])] = startTime

                if len(storeTimes) == 0:
                    startTimePerDay = [000000,000000,000000,000000,000000,000000,000000]
                    endTimePerDay = [246060,246060,246060,246060,246060,246060,246060]
                for i in range(0,7): #closed
                    if startTimePerDay[i] is None:
                        startTimePerDay[i] = 000000
                        endTimePerDay[i] = 000000
                
                # print(startTimePerDay)
                # print(endTimePerDay)

                query1 = """
                    SELECT timestamp_utc, getDayOfWeek(timestamp_utc) AS day, getTime(timestamp_utc) AS time, status
                    FROM StoreStatus WHERE store_id = {} 
                    ORDER BY timestamp_utc ASC
                """.format(store['store_id'])
                # next day end Time???

                query2 = """
                    SELECT getSortableTime(timestamp_utc) AS timestamp_utc, status
                    FROM ({}) 
                    WHERE 
                        (time >= {} AND time <= {} AND day = 0) 
                        OR (time >= {} AND time <= {} AND day = 1) 
                        OR (time >= {} AND time <= {} AND day = 2) 
                        OR (time >= {} AND time <= {} AND day = 3) 
                        OR (time >= {} AND time <= {} AND day = 4) 
                        OR (time >= {} AND time <= {} AND day = 5) 
                        OR (time >= {} AND time <= {} AND day = 6) 
                """.format(query1, startTimePerDay[0], endTimePerDay[0], 
                        startTimePerDay[1], endTimePerDay[1],
                        startTimePerDay[2], endTimePerDay[2],
                        startTimePerDay[3], endTimePerDay[3],
                        startTimePerDay[4], endTimePerDay[4],
                        startTimePerDay[5], endTimePerDay[5],
                        startTimePerDay[6], endTimePerDay[6])
                
                lastTimestampQuery = """
                    SELECT MAX(timestamp_utc) AS timestamp
                    FROM ({})
                """.format(query2)

                # calculate uptime in minutes past hour, past day, past week
                lastTimestamp = cur.execute(lastTimestampQuery).fetchone()['timestamp']
                print(lastTimestamp)
                if lastTimestamp is None:
                    continue
                lastTimestampDatetime = datetime.strptime(lastTimestamp, sortableTimestamp_fmt)
                oneHourBack = timedelta(hours=-1)
                oneDayBack = timedelta(days=-1)
                oneWeekBack = timedelta(weeks=-1)
                onehourbackTimestamp = lastTimestampDatetime + oneHourBack
                onedaybackTimestamp = lastTimestampDatetime + oneDayBack
                oneWeekbackTimestamp = lastTimestampDatetime + oneWeekBack

                businessTimestampsPastHour = cur.execute("""
                    SELECT timestamp_utc, status
                    FROM ({})
                    WHERE (timestamp_utc >= '{}' AND timestamp_utc <= '{}')""".format(query2, onehourbackTimestamp.strftime(sortableTimestamp_fmt), lastTimestamp)).fetchall()
                
                businessTimestampsPastDay = cur.execute("""
                    SELECT timestamp_utc, status
                    FROM ({})
                    WHERE (timestamp_utc >= '{}' AND timestamp_utc <= '{}')""".format(query2, onedaybackTimestamp.strftime(sortableTimestamp_fmt), lastTimestamp)).fetchall()
                businessTimestampsPastWeek = cur.execute("""
                    SELECT timestamp_utc, status
                    FROM ({})
                    WHERE (timestamp_utc >= '{}' AND timestamp_utc <= '{}')""".format(query2, oneWeekbackTimestamp.strftime(sortableTimestamp_fmt), lastTimestamp)).fetchall()
                
                uptime_PastHour, downtime_PastHour = getUptime(businessTimestampsPastHour)
                uptime_PastDay, downtime_PastDay = getUptime(businessTimestampsPastDay)
                uptime_PastWeek, downtime_PastWeek = getUptime(businessTimestampsPastWeek)
                
                report_rows.append([store['store_id'],uptime_PastHour ,uptime_PastDay, uptime_PastWeek, downtime_PastHour, downtime_PastDay, downtime_PastWeek])
            except:
                print("exception")
        print("saving file")

        # save CSV
        with open("store_uptime.csv", 'w') as csvfile:  
            writer = csv.writer(csvfile) 
            writer.writerow(report_fields)  
            writer.writerows(report_rows)  
        
        cur.execute("UPDATE ReportStatus SET status = '{}' WHERE reportId = '{}'".format("completed",str(report_id)))
        con.commit()

def getUptime(businessTimestamps):
    uptime = timedelta()
    downtime = timedelta()
    lastActivetime = datetime.strptime(businessTimestamps[0]['timestamp_utc'], sortableTimestamp_fmt)
    lastInactivetime = datetime.strptime(businessTimestamps[0]['timestamp_utc'], sortableTimestamp_fmt)
    timestampDateTime = datetime(1000,1,1)
    storeActive = True
    for timestamp in businessTimestamps:
        timestampDateTime = datetime.strptime(timestamp['timestamp_utc'], sortableTimestamp_fmt)
        # update uptime and downtime when status switches
        if timestamp['status'] == 'inactive' and storeActive:
            lastActivetime = timestampDateTime
            uptime += timestampDateTime - lastInactivetime
        elif timestamp['status'] == 'active' and not storeActive:
            lastInactivetime = timestampDateTime
            downtime += timestampDateTime - lastActivetime

        if timestamp['status'] == 'active':
            storeActive = True
        elif timestamp['status'] == 'inactive':
            storeActive = False

    # Add final status time block
    if storeActive:
        uptime += timestampDateTime - lastInactivetime
    else:
        downtime += timestampDateTime - lastActivetime
    uptime_in_seconds = uptime.total_seconds()
    uptime_in_minutes = round( uptime_in_seconds / 60 )
    downtime_in_seconds = uptime.total_seconds()
    downtime_in_minutes = round( downtime_in_seconds / 60 )
    return uptime_in_minutes, downtime_in_minutes
        

# curl -X POST -H "Content-Type: application/json" -d '{"report_id"="0e4767f0-dd2e-4dcf-a7d3-bea86f2cb1fc"}' localhost:5000/get_report
# get status of the report generation and send the report back if generated
@app.post("/get_report")
def get_report():
    report_id = request.get_json()["report_id"]
    with sql.connect("StoreDatabase.db") as con:
        con.row_factory = sql.Row
        cur = con.cursor()
        cur.execute("SELECT status FROM ReportStatus WHERE reportId = '{}'".format(report_id))
        status = cur.fetchone(); 
        con.commit()
        if (status['status'] == "completed"):
            response = send_from_directory(directory='.', filename='store_uptime.csv')
            response.headers['status'] = 'completed'
            return response
        else:
            return str(status['status'])

def get_set_event_loop():
    try:
        return asyncio.get_event_loop()
    except RuntimeError as e:
        if e.args[0].startswith('There is no current event loop'):
            asyncio.set_event_loop(asyncio.new_event_loop())
            return asyncio.get_event_loop()
        raise e