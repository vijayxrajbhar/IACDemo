import json
import os
import sys
import math
import time
import sqlite3 as lite
import socket
import threading
import datetime
import psycopg2
import zmq

LatData=None
LongData=None

DeviceList=[]

#Receive Data from Gevent
context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect("tcp://127.0.0.1:6661")
socket.setsockopt(zmq.SUBSCRIBE, '')


def Latitude(data):
    try:
				#print data
				datas=data.split(",")
				result=None
				first=float(datas[0])
				first=str(first)
				degree=float(first[0:2])
				minute=float(first[2:4])/60
				seconds=first.split(".")
				second=float('.'+seconds[1])*60/3600
				direction=datas[1]
				if direction == "S":
						second= (second)*-1
				else:
						second= (second)*1
				result =float(degree+minute+second)
				LatData=result
				return str(result)
    except Exception, e:
				LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
				print str(LogDateTimeStamp) + " Error : " + str(e)
				return str(LatData)

def Longitude(data):

    try:
				#print data
				datas=data.split(",")
				result=None
				first=float(datas[0])
				first=str(first)
				degree=float(first[0:2])
				minute=float(first[2:4])/60
				seconds=first.split(".")
				second = float('.'+seconds[1])*60/3600
				direction=datas[1]
				if direction == "W":
						second= (second)*-1
				else:
						second= (second)*1
				result =float(degree+minute+second)
				LongData=result
				return str(result)
    except Exception, e:
				LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
				print str(LogDateTimeStamp) + " Error : " + str(e)
				return str(LongData)



def ExecuteQry(Qry):
	try:
		con = psycopg2.connect(dbname='DIAL100-Test', user='postgres',password='postgres', host='localhost')         
		cur = con.cursor()	
		cur.execute(Qry)		
		con.commit()
		return "1"
	except Exception, e:
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))	
		print str(LogDateTimeStamp) + " Error ExecuteQry : " + str(e)
		con.close()


def ExecuteScaler(Qry):
	try:
		conn = psycopg2.connect(dbname='DIAL100-Test', user='postgres',password='postgres', host='localhost')
		cur = conn.cursor()	
		cur.execute(Qry)
		rows = cur.fetchone()
		return rows[0]
	except Exception, e:
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))	
		print str(LogDateTimeStamp) + " Error : " + str(e)	
		conn.close()
		
	finally:        
		if conn:    
			conn.close()


def UpdateUnit_Lat_Lon(deviceimei,lat,lon,datetime):
	try:
		UpdateQry = "update unit_master set unit_lat = '"+str(lat)+"', unit_long ='"+str(lon)+"', unitlast_timestamp ='"+str(datetime)+"' where unit_deviceimei='"+str(deviceimei)+"';"
		print UpdateQry
		chkresult = ExecuteQry(UpdateQry)

	except Exception, e:
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))	
		print str(LogDateTimeStamp) + " Error UpdateUnit_Lat_Lon : " + str(e)



def ConvertGMTtoIND(Gps_Date, Gps_Time):
	from datetime import datetime, timedelta
	import datetime 
	strGPSDATE = str(Gps_Date)+" "+str(Gps_Time)
	#print "GMT : "+str(strGPSDATE)
	getDateTimeFormet = " Wrong"
	#print len(strGPSDATE)
	if(len(strGPSDATE) >= 14):
		strGetdate = str(strGPSDATE[:8])
		strGetday = str(strGetdate[:2])
		strGetmonth = str(strGetdate[2:])[:2]
		strGetyear = str(strGPSDATE[4:])[:4]
		strGetTimeStamp = strGPSDATE[9:]
		if(len(strGetTimeStamp) == 5):
			strGetTimeStamp = "0"+str(strGetTimeStamp)
		strGetHours = str(strGetTimeStamp[:2])
		strGetMinutes = str(strGetTimeStamp[2:])[:2]
		strGetSeconds = str(strGetTimeStamp[4:])[:2]
		getDateTimeFormet = datetime.datetime(int(strGetyear), int(strGetmonth), int(strGetday), int(strGetHours), int(strGetMinutes), int(strGetSeconds)) + timedelta(hours=5,minutes=30,seconds=0)
		#print "INDIA : "+str(getDateTimeFormet)
	elif(len(GPSDateTime) == 13):
		strGetdate = str(GPSDateTime[:8])
		strGetday = str(strGetdate[:2])
		strGetmonth = str(strGetdate[2:])[:2]
		strGetyear = str(GPSDateTime[4:])[:4]
		strGetTimeStamp = GPSDateTime[9:]
		strGetHours = str(strGetTimeStamp[:1])
		strGetMinutes = str(strGetTimeStamp[1:])[:1]
		strGetSeconds = str(strGetTimeStamp[2:])[:2]
		getDateTimeFormet = datetime.datetime(int(strGetyear), int(strGetmonth), int(strGetday), int(strGetHours), int(strGetMinutes), int(strGetSeconds)) + timedelta(hours=5,minutes=31,seconds=6)

	return getDateTimeFormet


def CommonFunction(stringData):
	try:
		result = stringData.split(',')
		locationheader = str(result[0])
		#print locationheader
		if str(locationheader[1:]).upper() ==  '$LOC':
			Demo(stringData)
		elif str(locationheader[1:]).upper() ==  '$BAK':
			Demo(stringData)
		elif str(locationheader[1:]).upper() ==  '$RMV':
			Demo(stringData)
		elif str(locationheader[1:]).upper() ==  '$BTL':
			Demo(stringData)
		elif str(locationheader[1:]).upper() ==  '$TMP':
			Demo(stringData)
		elif str(locationheader[1:]).upper() ==  '$INR':
			Demo(stringData)
		elif str(locationheader[1:]).upper() ==  '$INA':
			Demo(stringData)
		elif str(locationheader[1:]).upper() ==  '$INS':
			Demo(stringData)
		elif str(locationheader[1:]).upper() ==  '$INC':
			Demo(stringData)
		elif str(locationheader[1:]).upper() ==  '$SOS':
			Demo(stringData)
		elif str(locationheader[1:]).upper() ==  '$RCV':
			Demo(stringData)
		else:
			print stringData
	except Exception, e:
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
		print str(LogDateTimeStamp) + " CommonFunction : " + str(e)

def CalculateDistanceKM(lon1, lat1, lon2, lat2):
    try:
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        km = distance((lat1, lon1), (float(lat2), float(lon2)))
        return km
    except Exception,e:
        LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
        print str(LogDateTimeStamp) + " CalculateDistanceKM : " + str(e)

def distance(origin, destination):
    try:
        import math
        #print str(origin) + " ,destination " +str(destination)
        lat1, lon1 = origin
        lat2, lon2 = destination
        radius = 6371 # km

        dlat = math.radians(lat2-lat1)
        dlon = math.radians(lon2-lon1)
        #print dlat
        a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
        #print a
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = radius * c
        #print d
        return d
    except Exception,e:
        LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
        print str(LogDateTimeStamp) + " distance : " + str(e)

def UpdateDeviceDistion(imei,lat1,long1):
    try:
        strQuery = "Select latitude,longitude from device_log where device_iemi = '"+str(imei)+"' order by updated_time desc limit 1;" #command='$loc' and
        row = ReturnData(strQuery)
        print len(row)
        dist = 0.0
        if(len(row)!=0):
			for item in row:
				lat2 = float(item[0])
				long2 = float(item[1])

			dist =  CalculateDistanceKM(round(float(long1),4), round(float(lat1),4), long2, lat2)
			print str(long2)  + "-----" + str(lat2)+"-----imei new "+str(imei)+ "= "+ str(round(float(long1),4))  + "-----" + str(round(float(lat1),4)) +"====="+str(dist)
			return dist
        else:
			return dist
    except Exception,e:
        LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
        print str(LogDateTimeStamp) + " UpdateDeviceDistion : " + str(e)


def insert_log_data(locationheader,Device_Iemi,Gps_Date,Gps_Time,Gps_Fix,dan_LATITUDE, dan_LONGITUDE, SpeedKPH,Heading,Cell_Id_Signal,
                    Signal_Strength,No_of_sat,Battery_level,Ignition_status,Input_status,Temper_status,Software_ver,Actual_DateTime,UpdateDate_time, Packets):
	con= None
	try:        
		#print data
		dbname = "/home/www-data/web2py/applications/welcome/databases/" + 'Device_History.sqlite'   #_'+time.strftime("%Y%m%d")+'.sqlite'
		if not os.path.exists(dbname):            
			con = lite.connect(dbname)
			cur =con.cursor()
			print "DataBase Created " + str(dbname)
			Qry ="Create TABLE device_log(id INTEGER PRIMARY KEY AUTOINCREMENT,command  TEXT,device_iemi TEXT,gps_Date Text,gps_time int,gps_fixed int  ,latitude Real,longitude Real,gps_speed Text,heading Real,cell_id int,signal_strength int,no_of_sat int, battery_level text,ignition_status int,input_status text,temper_status text,software_ver text,updated_time TIMESTAMP ,incident_no text,actual_datetime TIMESTAMP, packets text)"
			cur.execute(Qry)
			strIndex = "CREATE INDEX idx_device on device_log (command,device_iemi, updated_time, actual_datetime)"
			cur.execute(strIndex)
			Qry="INSERT INTO device_log(command,device_iemi,gps_Date,gps_time,gps_fixed,latitude ,longitude,gps_speed,heading ,cell_id,signal_strength,no_of_sat,battery_level,  ignition_status,input_status,temper_status,software_ver,updated_time,incident_no,actual_datetime) values ('"+str(locationheader)+"','"+str(Device_Iemi)+"','"+str(Gps_Date)+"',  '"+str(Gps_Time)+"','"+str(Gps_Fix)+"','"+str(dan_LATITUDE)+"',  '"+str(dan_LONGITUDE)+"','"+str(SpeedKPH)+"','"+str(Heading)+"',  '"+str(Cell_Id_Signal)+"','"+str(Signal_Strength)+"','"+str(No_of_sat)+"', '"+str(Battery_level)+"','"+str(Ignition_status)+"','"+str(Input_status)+"', '"+str(Temper_status[:-1])+"','"+str(Software_ver)+"','"+str(UpdateDate_time)+"','0','"+str(Actual_DateTime)+"',"+str(Packets)+") "
			#print Qry
			#UpdateUnit_Lat_Lon(str(Device_Iemi),str(dan_LATITUDE),str(dan_LONGITUDE),str(Actual_DateTime))
			cur.execute(Qry)
			con.commit()

		else: 
			con = lite.connect(dbname)
			cur =con.cursor()
            		print 'Check if device_log table exists in the database:'
            		listOfTables = cur.execute("""SELECT tbl_name FROM sqlite_master WHERE type='table' AND tbl_name='device_log'; """).fetchall()
            		if listOfTables == []:
                		print 'Table not found!'
                		Qry ="Create TABLE device_log(id INTEGER PRIMARY KEY AUTOINCREMENT,command  TEXT,device_iemi TEXT,gps_Date Text,gps_time int,gps_fixed int  ,latitude Real,longitude Real,gps_speed Text,heading Real,cell_id int,signal_strength int,no_of_sat int, battery_level text,ignition_status int,input_status text,temper_status text,software_ver text,updated_time TIMESTAMP ,incident_no text,actual_datetime TIMESTAMP, packets text)"
                		cur.execute(Qry)
                		strIndex = "CREATE INDEX idx_device on device_log (command,device_iemi, updated_time, actual_datetime)"
                		cur.execute(strIndex)

			if (str(locationheader).lower()=='$ina')or(str(locationheader).lower()=='$inr')or(str(locationheader).lower()=='$ins')or(str(locationheader).lower()=='$inc') or (str(locationheader).lower()=='$rcv'):
				Qry="INSERT INTO "\
				"device_log(command,device_iemi,gps_Date,gps_time,gps_fixed,latitude"\
				",longitude,gps_speed,heading ,cell_id,signal_strength,no_of_sat,battery_level,"\
				" ignition_status,input_status,temper_status,software_ver,updated_time,incident_no,actual_datetime, packets) values"\
				"('"+str(locationheader)+"','"+str(Device_Iemi)+"','"+str(0)+"',"\
				" '"+str(0)+"','"+str(0)+"','"+str(0)+"',"\
				" '"+str(0)+"','"+str(0)+"','"+str(0)+"',"\
				" '"+str(0)+"','"+str(0)+"','"+str(0)+"', "\
				" '"+str(0)+"','"+str(0)+"','"+str(0)+"', "\
				" '"+str(0)+"','"+str(0)+"','"+str(UpdateDate_time)+"','0','"+str(UpdateDate_time)+"',"+str(Packets)+") " 
				#print Qry
				#UpdateUnit_Lat_Lon(str(Device_Iemi),str(dan_LATITUDE),str(dan_LONGITUDE),str(Actual_DateTime))
				cur.execute(Qry)
				con.commit() 
				
			else:
				#count = 1
				#if(count == 1):
				#	strIndex = "CREATE INDEX idx_device on device_log (command,device_iemi, updated_time)"
				#	cur.execute(strIndex)
				#	qryupdate = "Alter table device_log add column incident_no text"
				#	cur.execute(qryupdate)
				#    	con.commit()
				#	count = count + 1
				
				Qry="INSERT INTO device_log(command,device_iemi,gps_Date,gps_time,gps_fixed,latitude ,longitude,gps_speed,heading ,cell_id,signal_strength,no_of_sat,battery_level,  ignition_status,input_status,temper_status,software_ver,updated_time,incident_no,actual_datetime, packets) values ('"+str(locationheader)+"','"+str(Device_Iemi)+"','"+str(Gps_Date)+"',  '"+str(Gps_Time)+"','"+str(Gps_Fix)+"','"+str(dan_LATITUDE)+"',  '"+str(dan_LONGITUDE)+"','"+str(SpeedKPH)+"','"+str(Heading)+"',  '"+str(Cell_Id_Signal)+"','"+str(Signal_Strength)+"','"+str(No_of_sat)+"', '"+str(Battery_level)+"','"+str(Ignition_status)+"','"+str(Input_status)+"', '"+str(Temper_status[:-1])+"','"+str(Software_ver)+"','"+str(UpdateDate_time)+"','0','"+str(Actual_DateTime)+"',"+str(Packets)+") "
				#print Qry
				#UpdateUnit_Lat_Lon(str(Device_Iemi),str(dan_LATITUDE),str(dan_LONGITUDE),str(Actual_DateTime))
				cur.execute(Qry)
				con.commit()
		return "1"

	except lite.DatabaseError, e:
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))	
		print str(LogDateTimeStamp) + " DatabaseError Error : " + str(e)
		sys.stdout.flush() 
		con.close()
		return "0"
	except Exception, e:
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))	
		print str(LogDateTimeStamp) + " Exception Error : " + str(e)
		sys.stdout.flush() 
		con.close()
		return "0"
	finally:
		if con:
			con.close()
			
			
			
def Demo(stringData):
	from datetime import datetime, timedelta
	import datetime 
	try:
		result = stringData.split(',')
		print stringData
		if len(result) >= 18:
			dan_location = str(result[0]) # $loc, #$bak, $rmv, $btl, $tmp, $smt, $btt, $ion, $iof, $stp, $pfa
			dan_IMEI =  str(result[1])#869309013800417
			dan_DATE = str(result[2])#08032014
			dan_TIME = str(result[3])#094459
			dan_GPSFix = str(result[4])#1
			dan_LATITUDE = str(result[5])#2826.1956
			dan_LAT_DIR = str(result[6])#N
			dan_LONGITUDE = str(result[7])#07659.7690
			dan_LONG_DIR = str(result[8])#E
			dan_SPEED = '0.0'
			if (str(result[9]) != ''):
				dan_SPEED = str(result[9])#0.0
			dan_HEADING = '0.0'
			if (str(result[10]) != ''):
			   dan_HEADING = str(result[10])#2.5
			dan_CELL_ID ='AA01'
			if (str(result[11]) != ''):
			   dan_CELL_ID = str(result[11])#4441
			dan_SIGNAL_STRENGTH = '00'
			if (str(result[12]) != ''):
			   dan_SIGNAL_STRENGTH = str(result[12])#31

			dan_NO_OF_SATELLITE_USED = '0'
			if (str(result[13]) != ''):
				dan_NO_OF_SATELLITE_USED = str(result[13])#6
			dan_BATT_LEVEL = '00'
			if (str(result[14]) != ''):
			   dan_BATT_LEVEL = str(result[14])#95
			dan_IGN_STATUS ='0'
			if (str(result[15]) != ''):
			   dan_IGN_STATUS = str(result[15])#1
			dan_DIGITAL_INPUT_STATUS = 'LLLL'
			if (str(result[16]) != ''):
			   dan_DIGITAL_INPUT_STATUS = str(result[16])#LLLL

			dan_TAMPERING_STATUS = 'NNN'
			if (str(result[17]) != ''):
			   dan_TAMPERING_STATUS = str(result[17])#NNTN
			   
			getDateTimeFormet = ConvertGMTtoIND(str(dan_DATE),str(dan_TIME))
			#print str(str(dan_location[1:]))+ ","+ str(dan_IMEI)+ ","+ str(dan_DATE)+ ","+ str(dan_TIME)+ ","+  str(dan_GPSFix)+ ","+ str(dan_LATITUDE)+ ","+ str(dan_LAT_DIR)+ ","+ str(dan_LONGITUDE)+ ","+ str(dan_LONG_DIR)+ ","+  str(dan_SPEED)+ ","+ str(dan_HEADING)+ ","+ str(dan_CELL_ID)+ ","+ str(dan_SIGNAL_STRENGTH)+ ","+  str(dan_NO_OF_SATELLITE_USED)+ ","+ str(dan_BATT_LEVEL)+ ","+ str(dan_IGN_STATUS)+ ","+ str(dan_DIGITAL_INPUT_STATUS)+ ","+   str(dan_TAMPERING_STATUS)+","+str(getDateTimeFormet)
			Updated_Time = str(time.strftime("%Y-%m-%d %H:%M:%S"))
			
			floatlat =None
			floatlong=None
			Software_ver = '0.0'
		  
			try:
				if(str(result[5])!= ''):
						if(str(result[7])!= ''):
							floatlat = Latitude(result[5]+','+result[6])
							floatlong = Longitude(result[7]+','+result[8])
							insert_log_data(str(dan_location[1:]),str(dan_IMEI),str(dan_DATE),str(dan_TIME), str(dan_GPSFix),str(floatlat),str(floatlong), dan_SPEED,dan_HEADING,str(dan_CELL_ID), str(dan_SIGNAL_STRENGTH),str(dan_NO_OF_SATELLITE_USED), str(dan_BATT_LEVEL),str(dan_IGN_STATUS),str(dan_DIGITAL_INPUT_STATUS),str(dan_TAMPERING_STATUS),Software_ver,str(getDateTimeFormet),str(Updated_Time), str(stringData))
							Dist = 0.0
			except Exception,e:
				LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
				print str(LogDateTimeStamp) + " Demo Try : " + str(e)
		else:
			if len(result) >= 4:
				Updated_Time = str(time.strftime("%Y-%m-%d %H:%M:%S"))
				dan_location = str(result[0]) # $loc, #$bak, $rmv, $btl, $tmp, $smt, $btt, $ion, $iof, $stp, $pfa
				dan_IMEI =  str(result[1])#869309013800417
				#insert_log_data(str(dan_location[1:]),str(dan_IMEI),0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,str(Updated_Time),str(Updated_Time), str(stringData))
				#print stringData

	except Exception,e:
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
		print str(LogDateTimeStamp) + " DENTracker : " + str(e)
		print stringData

def RecvData():
    if(len(DeviceList) > 0):
        msg = DeviceList.pop(0)
        CommonFunction(msg)

if __name__ == '__main__':
	sys.stdout = os.fdopen(sys.stdout.fileno(),'w',0)
	try:
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
		print str(LogDateTimeStamp) + " Start Script Name  ='2_1_databasewrite.py' Insert device_log data "
		sys.stdout.flush()
		while True:
			msg = socket.recv_json()
			#print msg
			DeviceList.append(str(msg))
			RecvData()

	except KeyboardInterrupt:
		socket.close()
		sys.exit(0)

