#!/bin/sh
if [ "$REAL_DATABASE" = "" ]; then
	until (nc -z -v -w30 db 3306 2> /dev/null)
	do
	  echo "Waiting for database connection..."
	  sleep 5
	done
else
	echo "Skipping db wait"
fi
/usr/local/spark-2.4.3-bin-hadoop2.7/bin/spark-submit --jars jars/mysql-connector-java-5.1.48-bin.jar src/main/python/detect_fraud/app.py --numofcores 6
