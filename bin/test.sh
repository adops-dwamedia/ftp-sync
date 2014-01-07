#!/usr/bin/env bash

SQL_PATH="../SQL/"
DATA_PATH="../downloaded/"
LOG_PATH="../logs/"
TMP_PATH="../tmp/"

USER="tomb"
HOST="localhost"
DB="DWA_SF_Cookie"
PW=`cat ../../pw/mysql`

# MySQL inserts. Unzip files, dynamically generate SQL to import each contained csv.
declare -a arr=("Conversion" "Standard")
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



