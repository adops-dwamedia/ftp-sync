#!/bin/bash

SQL_PATH="../SQL/"
DATA_PATH="../downloaded/"
LOG_PATH="../logs/"
TMP_PATH="../tmp/"

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

