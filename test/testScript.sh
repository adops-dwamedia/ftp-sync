while read l; do
	echo "Checking if i already have $l"
	a=`cat testRef | grep $l`
	
	if [ -z `cat testRef | grep $l` ] # if string a has zero length
	then
		echo "You should download $l"
	fi


done < testLis
