#!/usr/bin/env bash

HOME_PATH="/usr/local/ftp_sync/bin"
SQL_PATH="/usr/local/ftp_sync/SQL/"
DATA_PATH="/usr/local/var/ftp_sync/downloaded/"
LOG_PATH="/usr/local/ftp_sync/logs/"
TMP_PATH="/usr/local/ftp_sync/var/"

USER="tomb"
HOST="localhost"
DB="DWA_SF_Cookie"
PW=`cat /usr/local/pw/mysql`

# first, retrieve list of available files from server. 
echo "retrieving list of files"
ftp -pid ftp.platform.mediamind.com > $LOG_PATH/transfer.log 2>> $LOG_PATH/ftpErrors.log << EOF
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
echo "retrieving files"
# retrieve files. 

ftp -vpi ftp.platform.mediamind.com < $TMP_PATH/downloadCmds 2>> $LOG_PATH/ftpErrors.log >> $LOG_PATH/ftpLog
if [ "$?" -ne 0 ]
then
	echo "Errors detected in ftp process, exiting."
	exit 
fi

# LOG filenames that were downloaded!

# Import downloaded files to database. Separate Rich, Standard, and Conversion 
mv $DATA_PATH/*Rich*.zip $DATA_PATH/Rich 2> $LOG_PATH/mv_log
mv $DATA_PATH/*Standard*.zip $DATA_PATH/Standard 2> $LOG_PATH/mv_log
mv $DATA_PATH/*Conversion*.zip $DATA_PATH/Conversion 2> $LOG_PATH/mv_log
mv $DATA_PATH/*Match*.zip $DATA_PATH/Match 2> $LOG_PATH/mv_log


# MySQL inserts. Unzip files, dynamically generate SQL to import each contained csv.



echo "inserting data into database"
declare -a arr=("Rich" "Conversion" "Standard")
for i in ${arr[@]}
do
#	echo $i

for f in $DATA_PATH/$i/*.zip; do
	echo $f
	filename=`echo $f | sed 's:.*/::'`
	unzip -xu -d$TMP_PATH/unzip/$i $f >$LOG_PATH/unzip.log 2> $LOG_PATH/unzipErrors.log
	for gg in $TMP_PATH/unzip/$i/*.csv; do
		echo $gg
		echo "started at:"
		echo `date`
		mv $gg MM_$i
		mysqlimport -u$USER -p$PW --local --ignore-lines=1 --ignore --fields-terminated-by="," --lines-terminated-by="\n" $DB MM_$i
		if [ "$?" -eq 0 ]
		
			then
				mysql -h$HOST -u$USER -p$PW $DB -e "INSERT IGNORE INTO import_log VALUE ('$filename', CURRENT_TIMESTAMP())"
				#rm $TMP_PATH/tmpSql.sql
				rm -f MM_$i
#				mv $gg $TMP_PATH/unzip/$i/inSQL
		fi
		
	done
	##rm -rf $TMP_PATH/unzip
	mv $f $DATA_PATH/${i}/inSQL 2>> $LOG_PATH/mv_log
done
done
exit
for f in $DATA_PATH/Match/*.zip; do	
	filename=`echo $f | sed 's:.*/::'`
	unzip -xu -d$DATA_PATH/Match/unzipped $f >>$LOG_PATH/unzip.log 2>> $LOG_PATH/unzipErrors.log
	if [ "$?" -eq 0 ]
		then 
			##rm $f
			mysql -h$HOST -u$USER -p$PW $DB -e "INSERT IGNORE INTO import_log VALUE ('$filename', CURRENT_TIMESTAMP())"
				
	fi
done

python $HOME_PATH/match.py

if [ "$?" -eq 0 ]
	then
	echo "errors detected in match file creation"
		#rm -f $DATA_PATH/Match/unzipped/*.csv
fi
