#!/usr/bin/env bash

SQL_PATH="../SQL/"
DATA_PATH="/usr/local/var/ftp_sync/downloaded/"
LOG_PATH="../logs/"
TMP_PATH="../var/"

USER="tomb"
HOST="localhost"
DB="DWA_SF_Cookie"
PW=`cat ../../pw/mysql`

for f in $DATA_PATH/Match/*.zip; do
	echo "unzip {$f}"
	unzip -xu -d$DATA_PATH/Match/unzipped $f >>$LOG_PATH/unzip.log 2>> $LOG_PATH/unzipErrors.log
	echo $?
	if [ "$?" -eq 0 ]
		then 
			rm $f
	fi
done

python match.py


