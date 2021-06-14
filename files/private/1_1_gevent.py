import gevent
from gevent import socket
from gevent.server import StreamServer
import datetime
import time
import os
#import zmq
#from zmq.eventloop import ioloop
import sys
import socket
import threading
import SocketServer
import sys
import json
from gevent import Timeout
#from zmq import green as zmq
import zmq.green as zmq
import signal,sys

seconds = 5
#fileobj  = ""
#Receive data from device on Port 1650 
HOST, PORT = socket.gethostbyname(socket.gethostname()), 1650 # "172.31.28.36", 1650  #"192.168.5.155", 1650
print HOST
# Send data on ZMQ Pub socket on Port 5551
zmqsendport="tcp://127.0.0.1:6661" #192.168.2.56
	    
# Set up ZMQ PUB Socket
context = zmq.Context()
zmqsocket = context.socket(zmq.PUB)
zmqsocket.bind(zmqsendport)

def echo(sock, address):
	try:
		fileobj = sock.makefile()
		fileobj.flush()
		while True:
			try :
				try:
					timeout = Timeout(seconds)
					timeout.start()
					line = fileobj.readline()
					fileobj.flush()
				except Timeout:
					timeout.cancel()
					fileobj.close()
					#sock.shutdown(socket.SHUT_WR)
					sock.close()
					break
				except Exception:
					timeout.cancel()
					fileobj.close()
					#sock.shutdown(socket.SHUT_WR)
					sock.close()
					break
				if not line:
					timeout.cancel()
					fileobj.close()
					#sock.shutdown(socket.SHUT_WR)
					sock.close()
					break
				msg = repr(line)
				LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
				#print str(LogDateTimeStamp) + "," + str(address), " echoed ", msg
				#print address, msg
				sys.stdout.flush()
				
				if msg !='\r\n':
					if(str(msg[0:2]) == "'$"):
						modifyData = msg.split(',')
						if (str(modifyData[0]).upper() == "$SIFAIPT1NRML"):
							status = sendzmq(msg)
						elif  (str(modifyData[0]).upper() == "$SIFAIPT1TSTD"):
							status = sendzmq(msg)
						else:
							dan_GPSFix = str(modifyData[4])#1
							if(str(modifyData[1]) == "866873024191333"):
								print str(LogDateTimeStamp) , "->>>>>>>-->>>>>>>>>", msg
								status = sendzmq(msg)							
							elif(str(modifyData[1]) == "866873024191341"):
								print str(LogDateTimeStamp), "->>>>>>>-->>>>>>>>>", msg
								status = sendzmq(msg)						
							elif(str(modifyData[1]) == "861359037397492"):
								print str(LogDateTimeStamp), "->>>>>>>-->>>>>>>>>", msg
								status = sendzmq(msg)
								
							if(dan_GPSFix == "1"):
								print str(LogDateTimeStamp) + " echoed ", msg
								status = sendzmq(msg)
							else:
								if(str(modifyData[1]) == "861359037388442"):
									print str(LogDateTimeStamp) + " GPS Not Fix ", msg
					#else:
						#print str(LogDateTimeStamp) + " echoed ", msg
						#status = sendzmq(msg)
				else:
					print msg
							
			except Exception, e:
				timeout.cancel()
				fileobj.close()
				#sock.shutdown(socket.SHUT_WR)
				sock.close()
				LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
				print str(LogDateTimeStamp) + " echo Error : " + str(e) +" Actual Data = " +str(line)
				sys.stdout.flush()
			finally:
				timeout.cancel()
				fileobj.close()
				#sock.shutdown(socket.SHUT_WR)
				sock.close()
				LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
				#print str(LogDateTimeStamp) + " FINALLY CLOSE ONE  ", msg
	except Exception, e:
		timeout.cancel()
		fileobj.close()
		#sock.shutdown(socket.SHUT_WR)
		sock.close()
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
		print str(LogDateTimeStamp) + " echo Error : " + str(e) +" Actual Data = " +str(line)
		sys.stdout.flush()
	finally:
		timeout.cancel()
		fileobj.close()
		#sock.shutdown(socket.SHUT_WR)
		sock.close()
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
		#print str(LogDateTimeStamp) + " FINALLY CLOSE TWO  ", msg

def sendzmq(data):
	try:
		zmqsocket.send_json(data)
		return data
	except Exception, e:
		zmqsocket.close()
		context.term()
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
		print str(LogDateTimeStamp) + " sendzmq Error : " + str(e)
		sys.stdout.flush()
		#return e

if __name__ == '__main__':
	try:
		#do not accept more than 10000 connections,,,spawn=10000
		print "GJHGJHA"
		StreamServer.reuse_addr = 1
		server = StreamServer((HOST, PORT), echo)
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
		print str(LogDateTimeStamp) +  ' Starting Gevent Server on port 1650'
		sys.stdout.flush()
		server.serve_forever()
	except Exception, e: #KeyboardInterrupt: 
		LogDateTimeStamp = str(time.strftime("%Y-%m-%d %H:%M:%S"))
		print str(LogDateTimeStamp) +  ' Stopping Gevent Server on port 1650'
		sys.stdout.flush()
		print " Error : " + str(e)
		sys.stdout.flush()
		server.stop()
		sys.exit(0)

