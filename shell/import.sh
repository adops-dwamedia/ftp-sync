#!/bin/bash

for f in ../Conversion/*.zip; do
	echo $f
	unzip -xdu $f
	for gg in u/*.csv; do
		echo $gg
		echo "started at:"
		echo `date`
		sed -e "s@xxxxCSVFILExxxx@${gg}@g" ../SQL/Load_Conversion.sql > tmpSql.sql
		cat tmpSql.sql | mysql -utomb -pDW4mediatb -v --local-infile
		rm tmpSql.sql
		rm $gg
	done
	mv $f ../Conversion/inSQL
done
