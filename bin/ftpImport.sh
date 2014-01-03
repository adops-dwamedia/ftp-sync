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
rm $TMP_PATH/oFile


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

if [[ `cat $TMP_PATH/ftpErrors.log` != "" ]]
then
	echo "Errors detected in ftp process, exiting."
	exit 
fi
exit
rm downloadCmds
cat toDownload >> .downloaded


# Import downloaded files to database. Separate Rich, Standard, and Conversion 
mv *Rich* $DATA_PATH/Rich
mv *Standard* $DATA_PATH/Standard
mv *Conversion* $DATA_PATH/Conversion
mv *Match* $DATA_PATH/Match


# MySQL inserts. Unzip files, dynamically generate SQL to import each contained csv.
declare -a arr=("Conversion" "Rich" "Standard")
for i in ${arr[@]}
do
	echo $i


for f in ../$i/*.zip; do
	echo $f
	unzip -xdu $f
	for gg in u/*.csv; do
		echo $gg
		echo "started at:"
		echo `date`
		sed -e "s@xxxxCSVFILExxxx@${gg}@g" ../SQL/Load_${i}.sql > tmpSql.sql
		cat tmpSql.sql | mysql -utomb -pDW4mediatb -v --local-infile
		rm tmpSql.sql
		rm $gg
	done
	mv $f ../${i}/inSQL
done

done
exit












