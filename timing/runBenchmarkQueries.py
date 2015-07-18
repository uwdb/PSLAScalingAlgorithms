#imported some  from myria-web-py
import copy
import json
import os
import requests
from threading import Lock
import urllib
import subprocess
import time
import myria

hostname = 'localhost'
port = 8753

connection = myria.MyriaConnection(hostname=hostname, port=port)

qlist = [];
counter = 0;

#open the file to log the runtimes
f = open(os.path.expanduser("/root/timing/Type2Runtimes/4workersTPCH.txt"), 'w');

query_id = 0;

#for each query
while counter <= 20:

	averageTime = 0.0
	i = 0
	while i < 3:

		#call bash scripts
		subprocess.call(['/bin/bash',"/root/pslaData/postgresRestart.sh"]);
		subprocess.call(['/bin/bash',"/root/pslaData/clearOSCache.sh"]);
		print("postgres and os cleared")
		i = i + 1

		#if counter not in qlist:
		#for each json query
		json_data=open(os.path.expanduser("/root/sampleJoinQueries/Type2/4/json" + str(counter) + ".json"))
		data = json.load(json_data)
		json_data.close()

		#try running the query
		try:
			query_status = connection.submit_query(data)
			#query_url = 'http://%s:%d/execute?query_id=%d' % (hostname, port, query_status['queryId'])
			query_id = query_status['queryId']
			#print(json.dumps(connection.get_query_status(query_id)))
		except myria.MyriaError as e:
			print("MyriaError")
			print('Query #' + str(counter));

		status = (connection.get_query_status(query_id))['status']

		#keep checking, sleep a little
		while status!='SUCCESS':
			status = (connection.get_query_status(query_id))['status']
			print(status);
			if status=='SUCCESS':
				break;
			elif status=='KILLED':
				break;
			elif status=='ERROR':
	        	break;
			time.sleep(2);

	    if status == 'ERROR':
	        println('Error in query');
	        f.close();
	        sys.exit();


		#if the query was not killed then get the runtime and increase counter by one
		if status!='KILLED':
			print('Query #' + str(counter) + ' Finished with ' + status);
			totalElapsedTime = int((connection.get_query_status(query_id))['elapsedNanos'])
			averageTime = averageTime + totalElapsedTime
		else:
				print("Do over");
				print('Query #' + str(counter));


	timeSeconds = (averageTime / 3.0) /1000000000.0;
	print('Logging average runtime ' + str(timeSeconds));
	f.write(str(counter) + ',' + str(timeSeconds) + "\n");
	counter = counter + 1;
f.close();
