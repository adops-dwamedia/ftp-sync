#!/usr/bin/env bash

SQL_PATH="../SQL/"
DATA_PATH="../downloaded/"
LOG_PATH="../logs/"
TMP_PATH="../tmp/"

# first, retrieve list of available files from server. 
ftp -pivd ftp.platform.mediamind.com > $LOG_PATH/transfer.log << EOF
nlist . $TMP_PATH/oFile
quit
EOF


# strip out extra formatting
cat $TMP_PATH/oFile | grep 140102\.zip > $TMP_PATH/files.txt
rm -f $TMP_PATH/oFile


rm -f $TMP_PATH/toDownload
touch $TMP_PATH/toDownload

rm -f $TMP_PATH/downloadCmds
touch $TMP_PATH/downloadCmds


while read l; do
	# if filename is not in .downloaded, add to list of files to retrieve
	if [ -z `cat .downloaded | grep $l` ] 
	then
		echo "get $l $DATA_PATH/$l" >> $TMP_PATH/downloadCmds
		echo $l >> $TMP_PATH/toDownload
	fi
	
done < $TMP_PATH/files.txt

# retrieve files. 
ftp -vpi ftp.platform.mediamind.com < $TMP_PATH/downloadCmds 2> $LOG_PATH/ftpErrors.log

if [[ `cat $LOG_PATH/ftpErrors.log` != "" ]]
then
	echo "Errors detected in ftp process, exiting."
	exit 
fi

rm -f $TMP_PATH/downloadCmds
cat $TMP_PATH/toDownload >> .downloaded


# Import downloaded files to database. Separate Rich, Standard, and Conversion 
mv $DATA_PATH/*Rich*.zip $DATA_PATH/Rich
mv $DATA_PATH/*Standard*.zip $DATA_PATH/Standard
mv $DATA_PATH/*Conversion*.zip $DATA_PATH/Conversion
mv $DATA_PATH/*Match*.zip $DATA_PATH/Match



# MySQL inserts. Unzip files, dynamically generate SQL to import each contained csv.
declare -a arr=("Rich" "Standard" "Conversion")
for i in ${arr[@]}
do
	echo $i
for f in $DATA_PATH/$i/*.zip; do
	echo $f
	unzip -xdu $f >$LOG_PATH/unzip.log
	for gg in u/*.csv; do
		echo $gg
		echo "started at:"
		echo `date`
		sed -e "s@xxxxCSVFILExxxx@${gg}@g" ../SQL/Load_${i}.sql > tmpSql.sql
		cat tmpSql.sql | mysql -h184.105.184.30 -utomb -pDW4mediatb --local-infile # ADD ERROR CHECKING
		rm tmpSql.sql
		rm $gg
	done
	
	mv $f $DATA_PATH/${i}/inSQL
done

done



