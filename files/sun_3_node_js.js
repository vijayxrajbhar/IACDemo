//
// Script: 3_node.js
// Command: nodejs /home/www-data/web2py/applications/DIAL100/private/PatnaScript/3_node.js
// Description: 
//This script is used to pass notification  information from zmq to websocket using node.js. 
//



var http = require('http');
var sockjs = require('/home/ubuntu/node_modules/sockjs');
var node_static = require('/home/ubuntu/node_modules/sockjs'),
zmq = require('/home/ubuntu/node_modules/zeromq'),

//Receive Data on port 6662
sock = zmq.socket('pull');

//Receive Data on port 3399
unitsock = zmq.socket('pull');

//Alert Data on port 3398
alertsock = zmq.socket('pull');


//Incident Data on port 3400
ParkfenceAlrert = zmq.socket('pull')

//Incident Data on port 3401
UndueStoppage = zmq.socket('pull')

//Incident Data on port 3402
SpeedAlert = zmq.socket('pull')

//Incident Data on port 3403
RouteDeviationAlert = zmq.socket('pull')

//Incident Data on port 4001
temperingAlert = zmq.socket('pull')

UpdateCarrierStatus = zmq.socket('pull')

NodeJS_LoadUnload = zmq.socket('pull')


WEIGHBRIDGE_LoadUnload = zmq.socket('pull')



//Send Data to Websocket on Port 8888 
var port = 9999;
var host= '172.31.42.170'; //Change Server Private IP DHCP

//Receive ZMQ Messages on port 5552
connections = [];
sock.connect('tcp://127.0.0.1:6662');

//Receive ZMQ Notification on port 3399
unitsock.connect('tcp://127.0.0.1:3399');

//Receive ZMQ Alert Notification on port 3398
alertsock.connect('tcp://127.0.0.1:3398');

//Receive ZMQ Alert Notification on port 3400
ParkfenceAlrert.connect('tcp://127.0.0.1:3400');
	
//Receive ZMQ Alert Notification on port 3402
UndueStoppage.connect('tcp://127.0.0.1:3402');//Incident Notify

//Receive ZMQ Alert Notification on port 3401
SpeedAlert.connect('tcp://127.0.0.1:3401');//Unit Notify

//Receive ZMQ Alert Notification on port 3403
RouteDeviationAlert.connect('tcp://127.0.0.1:3403');//Unit Notify

//Receive ZMQ Alert Notification on port 4001
temperingAlert.connect('tcp://127.0.0.1:4001');//Unit Notify

//Receive ZMQ Alert Notification on port 4002
UpdateCarrierStatus.connect('tcp://127.0.0.1:4002');//Unit Notify

NodeJS_LoadUnload.connect('tcp://127.0.0.1:4111');
WEIGHBRIDGE_LoadUnload.connect('tcp://127.0.0.1:4112');



//////////////////////////////////////////////////////
NodeJS_Sodepur   = zmq.socket('pull')
NodeJS_Sodepur.connect('tcp://127.0.0.1:1501')
NodeJS_Satgram  = zmq.socket('pull')
NodeJS_Satgram.connect('tcp://127.0.0.1:1502')
NodeJS_Sripur  = zmq.socket('pull')
NodeJS_Sripur.connect('tcp://127.0.0.1:1503')
NodeJS_SPMines  = zmq.socket('pull')
NodeJS_SPMines.connect('tcp://127.0.0.1:1504')
NodeJS_SonepurBazari  = zmq.socket('pull')
NodeJS_SonepurBazari.connect('tcp://127.0.0.1:1505')
NodeJS_Rajmahal  = zmq.socket('pull')
NodeJS_Rajmahal.connect('tcp://127.0.0.1:1506')
NodeJS_Mugma  = zmq.socket('pull')
NodeJS_Mugma.connect('tcp://127.0.0.1:1507')
NodeJS_Salanpur  = zmq.socket('pull')
NodeJS_Salanpur.connect('tcp://127.0.0.1:1508')
NodeJS_Kenda  = zmq.socket('pull')
NodeJS_Kenda.connect('tcp://127.0.0.1:1509')
NodeJS_Jhanjhra  = zmq.socket('pull')
NodeJS_Jhanjhra.connect('tcp://127.0.0.1:1510')
NodeJS_Kajora  = zmq.socket('pull')
NodeJS_Kajora.connect('tcp://127.0.0.1:1511')
NodeJS_Kunustoria  = zmq.socket('pull')
NodeJS_Kunustoria.connect('tcp://127.0.0.1:1512')
NodeJS_Pandavaswar  = zmq.socket('pull')
NodeJS_Pandavaswar.connect('tcp://127.0.0.1:1513')
NodeJS_Bankola  = zmq.socket('pull')
NodeJS_Bankola.connect('tcp://127.0.0.1:1514')
NodeJS_LoadedCarrier = zmq.socket('pull')
NodeJS_LoadedCarrier.connect('tcp://127.0.0.1:1515');
NodeJS_UnLoadedCarrier = zmq.socket('pull')
NodeJS_UnLoadedCarrier.connect('tcp://127.0.0.1:1516');
//////////////////////////////////////////////////////

function CurrentDate()
{
	var CurrentDate = new Date();
	var dd = CurrentDate.getDate();
	var mm = CurrentDate.getMonth() + 1;
	var yyyy = CurrentDate.getFullYear();
	var HH = CurrentDate.getHours();
	var MM = CurrentDate.getMinutes();
	var SS = CurrentDate.getSeconds();
	var LogTimeStamp = yyyy + '-' + mm + '-' + dd + '  ' + HH + ':' +  MM + ':' + SS;
	return LogTimeStamp;
}

var echo = sockjs.createServer();
echo.on('connection', function(conn) {
     connections.push(conn);
    conn.on('data', function(message) {
      console.log('Got data: ' + message);
       for (var i=0; i<connections.length; i++) {
              connections[i].write(message);
        }
    });
    conn.on('close', function() {});
});

echo.on('error', function(exc){
    sys.puts(CurrentDate() + " echo  Exception : " + exc);
})

sock.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
     //console.log('sock: %s', msg.toString());
});

sock.on('error', function(exc){
    sys.puts(CurrentDate() + " sock  Exception : " + exc);
})

unitsock.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('other unit: %s', msg.toString());
});

unitsock.on('error', function(exc){
    sys.puts(CurrentDate() + " unitsock  Exception : " + exc);
})

alertsock.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Speak Alert: -------------------- %s', msg.toString());
});

alertsock.on('error', function(exc){
    sys.puts(CurrentDate() + " alertsock  Exception : " + exc);
})

ParkfenceAlrert.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
   //console.log('Park Fence Alert: %s', msg.toString());
})

ParkfenceAlrert.on('error', function(exc){
    sys.puts(CurrentDate() + " incidentsock  Exception : " + exc);
})

UndueStoppage.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Undue Stoppage Alert: %s', msg.toString());
})

UndueStoppage.on('error', function(exc){
    sys.puts(CurrentDate() + " incidentunitsock  Exception : " + exc);
})

SpeedAlert.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Speed  Alert: %s', msg.toString());
})

SpeedAlert.on('error', function(exc){
    sys.puts(CurrentDate() + " unitincidentsock  Exception : " + exc);
})

RouteDeviationAlert.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Route Deviation Alert: %s', msg.toString());
})

RouteDeviationAlert.on('error', function(exc){
    sys.puts(CurrentDate() + " generate_alerts_from_device  Exception : " + exc);
})

temperingAlert.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Temper Alert: %s', msg.toString());
})

temperingAlert.on('error', function(exc){
    sys.puts(CurrentDate() + " informalalert  Exception : " + exc);
})


UpdateCarrierStatus.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Update Carrier Status Alert: %s', msg.toString());
})

UpdateCarrierStatus.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})


NodeJS_LoadUnload.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Load Unload: %s', msg.toString());
})

NodeJS_LoadUnload.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})



WEIGHBRIDGE_LoadUnload.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Load Unload: %s', msg.toString());
})

WEIGHBRIDGE_LoadUnload.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})
///////////////////////////////////////////////////////////////////////////////

NodeJS_LoadedCarrier.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Load: %s', msg.toString());
})

NodeJS_LoadedCarrier.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})

NodeJS_UnLoadedCarrier.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Unload: %s', msg.toString());
})

NodeJS_UnLoadedCarrier.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})

NodeJS_Sodepur.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Unload: %s', msg.toString());
})
NodeJS_Sodepur.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})
NodeJS_Satgram.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Unload: %s', msg.toString());
})
NodeJS_Satgram.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})
NodeJS_Sripur.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Unload: %s', msg.toString());
})
NodeJS_Sripur.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})
NodeJS_SPMines .on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Unload: %s', msg.toString());
})
NodeJS_SPMines.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})
NodeJS_SonepurBazari .on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Unload: %s', msg.toString());
})
NodeJS_SonepurBazari.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})
NodeJS_Rajmahal.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Unload: %s', msg.toString());
})
NodeJS_Rajmahal.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})
NodeJS_Mugma.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Unload: %s', msg.toString());
})
NodeJS_Mugma.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})
NodeJS_Salanpur.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Unload: %s', msg.toString());
})
NodeJS_Salanpur.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})
NodeJS_Kenda.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Unload: %s', msg.toString());
})
NodeJS_Kenda.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})
NodeJS_Jhanjhra.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Unload: %s', msg.toString());
})
NodeJS_Jhanjhra.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})
NodeJS_Kajora.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Unload: %s', msg.toString());
})
NodeJS_Kajora.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})
NodeJS_Kunustoria.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Unload: %s', msg.toString());
})
NodeJS_Kunustoria.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})
NodeJS_Pandavaswar.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Unload: %s', msg.toString());
})
NodeJS_Pandavaswar.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})
NodeJS_Bankola.on('message', function(msg){
    for (var i=0; i<connections.length; i++) {
              connections[i].write(msg);
        }
    //console.log('Unload: %s', msg.toString());
})
NodeJS_Bankola.on('error', function(exc){
    sys.puts(CurrentDate() + " Update Carrier Status  Exception : " + exc);
})

///////////////////////////////////////////////////////////////////////////////


var server = http.createServer();
echo.installHandlers(server, {prefix:'/socket'});
server.listen(port, host);
