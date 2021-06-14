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

#Send Data NodeJS
sendcontext = zmq.Context()
sendsocket = sendcontext.socket(zmq.PUSH)
sendsocket.bind("tcp://127.0.0.1:6662")


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
		return "Error"

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
		return "Error"




def CommonFunction123(stringData):
	try:
		result = stringData.split(',')
		locationheader = str(result[0])
		if str(locationheader[1:]).upper() ==  '$LOC':
			DenTrackerDemo(stringData)
	except Exception, e:
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
		print str(LogDateTimeStamp) + " CommonFunction : " + str(e)


def ShiftCheckStatus(carrier_deviceimei):
    try:
        SelectQry = " SELECT CM.carrier_id, SM.shift_timing, SM.shift_timing_to, "\
                    " SM.shift_timing || ' to ' || SM.shift_timing_to as Timing   FROM carrier_master AS CM "\
                    " INNER JOIN shift_master AS SM on CM.carrier_dayshift = SM.id "\
                    " where CM.carrier_deviceimei = '"+str(carrier_deviceimei)+"' "\
                    " and ((to_timestamp(SM.shift_timing, 'HH24:MI') <= to_timestamp(to_char(current_timestamp, 'HH24:MI'), 'HH24:MI')) "\
                    " and (to_timestamp(SM.shift_timing_to, 'HH24:MI') >= to_timestamp(to_char(current_timestamp, 'HH24:MI'), 'HH24:MI'))) "
        result = ExcuteReturnAllData(SelectQry)
        startTime =''
        endTime = ''
        Carrier_id = ''
        Count = 0
        for item in result:
            Count = Count + 1
        return Count

    except psycopg2.DatabaseError, e:
        LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
        print str(LogDateTimeStamp) +  ' Undue_Stoppage() Database Error %s' % e
        sys.stdout.flush()

    except Exception, e:
        LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
        print str(LogDateTimeStamp) + " Undue_Stoppage() : " + str(e)
        sys.stdout.flush()


def ConvertGMTtoIND(Gps_Date, Gps_Time):
	from datetime import datetime, timedelta
	import datetime 
	strGPSDATE = str(Gps_Date)+" "+str(Gps_Time)
	#print "GMT : "+str(strGPSDATE)
	getDateTimeFormet = " Wrong"
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
		getDateTimeFormet = datetime.datetime(int(strGetyear), int(strGetmonth), int(strGetday), int(strGetHours), int(strGetMinutes), int(strGetSeconds)) + timedelta(hours=5,minutes=30,seconds=0)

	return getDateTimeFormet
	

def GetUnloadedStatus(DeviceImei):
	#Loadstatus = ''
	Loadstatus = ReturnFetchOne("Select RCM.carrier_loaded from carrier_master as CM   Inner Join area_master as AM on CM.carrier_area_id = AM.id   Inner Join route_carrier_map as RCM on CM.id = RCM.route_carrier_id where CM.carrier_deviceimei = '" + str(DeviceImei) + "';", "1")
	#print Loadstatus
	return Loadstatus

Counter = 0
CounterCarrierQuery = ""

def CheckStringValidooNot(stringexample):
	valid = set('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789$,.+-')
	return set(stringexample).issubset(valid)

def DANTracker(stringData):
	import numbers
	try:
		result = stringData.split(',')
		if len(result) >= 21:
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
				dan_TAMPERING_STATUS = 'NNNN'
			if (str(result[17]) != ''):
				dan_TAMPERING_STATUS = str(result[17])#NNTN
			dan_VECHICLE_STATUS = 'ACTIVE'
			if (str(result[18]) != ''):
				dan_VECHICLE_STATUS = str(result[18])#ACTIVE
			dan_ANALOG_INPUT = '0.00'
			if (str(result[19]) != ''):
				dan_ANALOG_INPUT = str(result[19])#0.15
			dan_SOFTWARE_VER = 'SW2.71'
			if (str(result[20]) != ''):
				dan_SOFTWARE_VER = str(result[20])#SW2.71
			dan_ODOMETER = '0'
			if (str(result[21]) != ''):
				dan_ODOMETER = str(result[21])#0
			Updated_Time = str(time.strftime("%Y-%m-%d %H:%M:%S"))
			floatlat =None
			floatlong=None
			actual_packets = str(stringData)
			
			if isinstance(int(dan_IMEI), int)&isinstance(float(dan_SPEED), float)&isinstance(float(dan_HEADING), float) &isinstance(int(dan_IGN_STATUS), int) :
				if ((int(len(dan_DATE))  <= 8)&(int(len(dan_DATE))  >= 6))|(isinstance(int(dan_DATE), int)):
						if((int(len(dan_TIME))  <= 6)&(int(len(dan_TIME))  >= 4))|(isinstance(int(dan_TIME), int)):
							try:
								if(str(result[5])!= ''):
									if(str(result[7])!= ''):
										floatlat = Latitude(result[5]+','+result[6])
										floatlong = Longitude(result[7]+','+result[8])
										if(isinstance(float(floatlat), float)):
											if(isinstance(float(floatlong), float)):
												ActualTimeDate = ConvertGMTtoIND(dan_DATE,dan_TIME)
												CarrierStatus = "Loaded" #GetUnloadedStatus(dan_IMEI)
												if(CarrierStatus == "Error"):
													CarrierStatus = "Error"
												
													#UpdateCarrierMaster(str(ActualTimeDate),str(floatlat), str(floatlong), str(dan_IMEI))
												MapData = {'MsgType':'LiveMap','Device':str(dan_IMEI),'Lat':str(floatlat),'Lon':str(floatlong),'Loc':str(dan_location[1:]),'Speed':str(dan_SPEED),'Datetime':str(ActualTimeDate),'SpeedStatus' :'0','IgnitionStatus':str(dan_IGN_STATUS),'CarrierStatus':str(CarrierStatus),'Head':str(dan_HEADING),'ReceivedDateTime':str(Updated_Time)}
												mylist=[]
												mylist.append(MapData)
												print mylist
												sendsocket.send_json(mylist)
											else:
												print "Longitude format is wrong " , isinstance(float(floatlong), float)
												#print float(floatlat)
										else:
											print "Latitude format is wrong " , isinstance(float(floatlat), float)
							except Exception, e:
								LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
								print str(LogDateTimeStamp) + " Demo Try : " + str(e)
						else:
							print "Time Format Wrong "+ str(stringData)
				else:
					print "Date Format Wrong "+ str(stringData)
			else:
				print "Not a correct format Device IMEI " + str(dan_IMEI)
			
			
	except Exception, e:
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
		print str(LogDateTimeStamp) + " DANTracker : " + str(e)
		print str(LogDateTimeStamp) + str(stringData)  + ", Total Lenghth : "+ str(len(result))
		sys.stdout.flush()



def CommonFunction(stringData):
	try:
		result = stringData.split(',')
		locationheader = str(result[0])
		if str(locationheader[1:]).upper() ==  '$LOC':
			DANTracker(stringData)
		
	except Exception, e:
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
		print str(LogDateTimeStamp) + " CommonFunction : " + str(e)





def RecvData():
	if(len(DeviceList) > 0):
		msg = DeviceList.pop(0)
		if(CheckStringValidooNot(msg[:-3][1:])):
			CommonFunction(msg)



if __name__ == '__main__':
	sys.stdout = os.fdopen(sys.stdout.fileno(),'w',0)
	try:
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
		print str(LogDateTimeStamp) + " Send Node JS Script Start"
		sys.stdout.flush()
		while True:
			msg = socket.recv_json()
			#print msg
			DeviceList.append(str(msg))			
			RecvData()
			
	except KeyboardInterrupt: 
		socket.close()			
		sys.exit(0)
