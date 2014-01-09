for f in $DATA_PATH/Match/; do
	unzip -xu -d$DATA_PATH/Match/ >>$LOG_PATH/unzip.log 2>> $LOG_PATH/unzipErrors.log
	if [ "$?" -eq 0 ]
		then 
			rm $f
	fi
done

python match.py


