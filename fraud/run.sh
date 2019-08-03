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
python src/main/python/detect_fraud/app.py
