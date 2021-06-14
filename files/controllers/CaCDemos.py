# -*- coding: utf-8 -*-
# try something like
@auth.requires_login()
def devices(): return dict(message="Device Communication List")

@auth.requires_login()
def device_history(): return dict(message="Device History")

#@auth.requires_login()
def livetracking(): return dict(message="hello from CaCDemos.py")

@auth.requires_login()
def replaylivetracking(): return dict(message="hello from CaCDemos.py")

import sqlite3
def GetTheDeviceHistory():
    import json
    SqliteConnection = None
    DATASET=[]
    try:
        DBPathName = "/home/www-data/web2py/applications/welcome/databases/"
        DBName = "Device_History.sqlite"
        Path = str(DBPathName) + str(DBName)
        SqliteConnection = sqlite3.connect(Path)# connect to the database
        cur = SqliteConnection.cursor() # instantiate a cursor obj
        SelectQuery = "SELECT command, device_iemi, latitude, longitude, gps_speed, heading, ignition_status, actual_datetime, updated_time FROM device_log Order by actual_datetime Desc;"
        cur.execute(SelectQuery)
        record = cur.fetchall()
        for i in record:
            command = str(i[0])
            device_iemi = str(i[1])
            latitude = str(i[2])
            longitude = str(i[3])
            gps_speed = str(i[4])
            heading = str(i[5])
            ignition_status = str(i[6])
            actual_datetime = str(i[7])
            updated_time = str(i[8])
            Result = {"Command":str(command), "device_iemi":str(device_iemi), "latitude":str(latitude), "longitude":str(longitude), "gps_speed":str(gps_speed), "heading":str(heading), "ignition_status":str(ignition_status), "actual_datetime":str(actual_datetime), "updated_time":str(updated_time)}
            DATASET.append(Result)
        cur.close()
    except sqlite3.Error as error:
        print("Error while connecting to sqlite", error)
    finally:
        if (SqliteConnection):
            SqliteConnection.close()
            print("The SQLite connection is closed")
    return json.dumps({'Result':DATASET,'Length':len(DATASET)})


import sqlite3
def GetTotalDeviceCommunicate():
    import json
    SqliteConnection = None
    DATASET=[]
    try:
        DBPathName = "/home/www-data/web2py/applications/welcome/databases/"
        DBName = "Device_History.sqlite"
        Path = str(DBPathName) + str(DBName)
        SqliteConnection = sqlite3.connect(Path)# connect to the database
        cur = SqliteConnection.cursor() # instantiate a cursor obj
        SelectQuery = "SELECT Distinct device_iemi FROM device_log;"
        cur.execute(SelectQuery)
        record = cur.fetchall()
        for i in record:
            device_iemi = str(i[0])
            SelectQuery = "SELECT latitude, longitude, actual_datetime, updated_time, gps_speed, heading, ignition_status FROM device_log Where device_iemi = '"+str(device_iemi)+"' Order By actual_datetime Desc Limit 1;"
            cur.execute(SelectQuery)
            record1 = cur.fetchall()
            for ii in record1:
                latitude = str(ii[0])
                longitude = str(ii[1])
                actual_datetime = str(ii[2])
                updated_time = str(ii[3])
                Result = {"device_iemi":str(device_iemi), "latitude":str(latitude), "longitude":str(longitude), "actual_datetime":str(actual_datetime), "updated_time":str(updated_time)}
                DATASET.append(Result)
        cur.close()
    except sqlite3.Error as error:
        print("Error while connecting to sqlite", error)
    finally:
        if (SqliteConnection):
            SqliteConnection.close()
            print("The SQLite connection is closed")
    return json.dumps({'Result':DATASET,'Length':len(DATASET)})
