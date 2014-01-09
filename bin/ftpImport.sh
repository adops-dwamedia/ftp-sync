#!/usr/bin/env bash

SQL_PATH="../SQL/"
DATA_PATH="/usr/local/var/ftp_sync/downloaded/"
LOG_PATH="../logs/"
TMP_PATH="../var/"

USER="tomb"
HOST="localhost"
DB="DWA_SF_Cookie"
PW=`cat ../../pw/mysql`

# first, retrieve list of available files from server. 
ftp -pid ftp.platform.mediamind.com > $LOG_PATH/transfer.log << EOF
nlist . $TMP_PATH/oFile
quit
EOF


# strip out extra formatting
cat $TMP_PATH/oFile | grep \.zip > $TMP_PATH/files.txt
#rm -f $TMP_PATH/oFile


rm -f $TMP_PATH/toDownload
touch $TMP_PATH/toDownload

rm -f $TMP_PATH/downloadCmds
touch $TMP_PATH/downloadCmds

rm -f $TMP_PATH/downloaded
mysql -h$HOST -u$USER -p$PW $DB -e "SELECT fileName FROM import_log" > $TMP_PATH/downloaded


while read l; do
	# if filename is not in .downloaded, add to list of files to retrieve
	if [ -z `cat $TMP_PATH/downloaded | grep $l` ] 
	then
		#echo "retrieving {$l}"
		echo "get $l $DATA_PATH/$l" >> $TMP_PATH/downloadCmds
		echo $l >> $TMP_PATH/toDownload
	fi
	
done < $TMP_PATH/files.txt
echo "unzip commands gathered"
# retrieve files. 

ftp -vpi ftp.platform.mediamind.com < $TMP_PATH/downloadCmds 2> $LOG_PATH/ftpErrors.log

if [[ `cat $LOG_PATH/ftpErrors.log` != "" ]]
then
	echo "Errors detected in ftp process, exiting."
	exit 
fi

# LOG filenames that were downloaded!

# Import downloaded files to database. Separate Rich, Standard, and Conversion 
mv $DATA_PATH/*Rich*.zip $DATA_PATH/Rich
mv $DATA_PATH/*Standard*.zip $DATA_PATH/Standard
mv $DATA_PATH/*Conversion*.zip $DATA_PATH/Conversion
mv $DATA_PATH/*Match*.zip $DATA_PATH/Match


# MySQL inserts. Unzip files, dynamically generate SQL to import each contained csv.
declare -a arr=("Rich" "Conversion" "Standard")
for i in ${arr[@]}
do
	echo $i

for f in $DATA_PATH/$i/*.zip; do
	filename=`echo $f | sed 's:.*/::'`
	unzip -xu -d$TMP_PATH/unzip $f >$LOG_PATH/unzip.log 2> $LOG_PATH/unzipErrors.log
	for gg in $TMP_PATH/unzip/*.csv; do
		echo $gg
		echo "started at:"
		echo `date`
		sed -e "s@xxxxCSVFILExxxx@${gg}@g" $SQL_PATH/Load_${i}.sql > tmpSql.sql
		cat tmpSql.sql | mysql -h$HOST -u$USER -p$PW $DB --local-infile # ADD ERROR CHECKING
		if [ "$?" -eq 0 ]
			then
				mysql -h$HOST -u$USER -p$PW $DB -e "INSERT IGNORE INTO import_log VALUE ('$filename', CURRENT_TIMESTAMP())"
				rm tmpSql.sql
				rm $gg
		fi
		
	done
	#rm -rf $TMP_PATH/unzip
	mv $f $DATA_PATH/${i}/inSQL
done
done

for f in $DATA_PATH/Match/*.zip; do	
	filename=`echo $f | sed 's:.*/::'`
	unzip -xu -d$DATA_PATH/Match/unzipped $f >>$LOG_PATH/unzip.log 2>> $LOG_PATH/unzipErrors.log
	if [ "$?" -eq 0 ]
		then 
			rm $f
			mysql -h$HOST -u$USER -p$PW $DB -e "INSERT IGNORE INTO import_log VALUE ('$filename', CURRENT_TIMESTAMP())"
				
	fi
done

python match.py

if [ "$?" -eq 0 ]
	then
		rm $DATA_PATH/Match/unzipped/*.csv
fi
