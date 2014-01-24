#!/usr/bin/env bash

print "beginning ftpImport.py"
home_path="/usr/local/ftp_sync/bin"
sql_path="/usr/local/ftp_sync/SQL/"
data_path="/usr/local/var/ftp_sync/downloaded/"
log_path="/usr/local/ftp_sync/logs/"
tmp_path="/usr/local/ftp_sync/var/"

user="tomb"
host="localhost"
db="DWA_SF_Cookie"
pw=os.popen('/usr/local/pw/mysql').read()

# first, retrieve list of available files from server. 
print "retrieving list of files"
os.system("""ftp -pid ftp.platform.mediamind.com > $log_path/transfer.log 2>> $log_path/ftpErrors.log << EOF
nlist . $tmp_path/oFile
quit
EOF""")

# Test LINE - limit to one day.
#cat ofile | grep 140120 > ofile2
#cat ofile2 > ofile
# END Test

# strip out extra formatting
os.system("cat $tmp_path/oFile | grep \.zip > $tmp_path/files.txt")
#rm -f $tmp_path/oFile


os.system("rm -f $tmp_path/toDownload")
os.system("touch $tmp_path/toDownload")

os.system("rm -f $tmp_path/downloadCmds")
os.system("touch $tmp_path/downloadCmds")

os.system("rm -f $tmp_path/downloaded")

####################################################################
function ftpImport()
function ftpCommands()
function csvImport()
function advertiserTables()
function matchTables()



####################################################################
mysql -h$HOST -u$USER -p$PW $DB -e "SELECT fileName FROM import_log" > $tmp_path/downloaded


while read l; do
	# if filename is not in .downloaded, add to list of files to retrieve
	if [ -z `cat $tmp_path/downloaded | grep $l` ] 
	then
		#print "retrieving {$l}"
		print "get $l $data_path/$l" >> $tmp_path/downloadCmds
		print $l >> $tmp_path/toDownload
	fi
	
done < $tmp_path/files.txt
print "retrieving files"
# retrieve files. 

#ftp -vpi ftp.platform.mediamind.com < $tmp_path/downloadCmds 2>> $log_path/ftpErrors.log >> $log_path/ftpLog
if [ "$?" -ne 0 ]
then
	print "Errors detected in ftp process, exiting."
	exit 
fi

# LOG filenames that were downloaded!

# Import downloaded files to database. Separate Rich, Standard, and Conversion 
mv $data_path/*Rich*.zip $data_path/Rich 2> $log_path/mv_log
mv $data_path/*Standard*.zip $data_path/Standard 2> $log_path/mv_log
mv $data_path/*Conversion*.zip $data_path/Conversion 2> $log_path/mv_log
mv $data_path/*Match*.zip $data_path/Match 2> $log_path/mv_log


# MySQL inserts. Unzip files, dynamically generate SQL to import each contained csv.



print "inserting data into database"
declare -a arr=("Rich" "Conversion" "Standard")
for i in ${arr[@]}
do
#	print $i

for f in $data_path/$i/*.zip; do
	print $f
	filename=`print $f | sed 's:.*/::'`
	unzip -xu -d $tmp_path/unzip/$i $f >$log_path/unzip.log 2> $log_path/unzipErrors.log
	for gg in $tmp_path/unzip/$i/*.csv; do
		print $gg
		print "started at:"
		print `date`
		
		
		if [ "$i"="Standard" ]
		then
			print "standard loop reached"
			tmpString=_tmp
			print MM_$i$tmpString
			mv $gg MM_$i$tmpString
			mysqlimport -u$USER -p$PW --local --ignore-lines=1 --ignore --fields-terminated-by="," --lines-terminated-by="\n" $DB MM_$i$tmpString
		else
			mv $gg MM_$i
			mysqlimport -u$USER -p$PW --local --ignore-lines=1 --ignore --fields-terminated-by="," --lines-terminated-by="\n" $DB MM_$i
		fi
		if [ "$?" -eq 0 ]
		
			then
				mysql -h$HOST -u$USER -p$PW $DB -e "INSERT IGNORE INTO import_log VALUE ('$filename', CURRENT_TIMESTAMP())"
				rm -f MM_$i
				rm -f MM_$i$tmpString				

		fi

	done

	mv $f $data_path/${i}/inSQL 2>> $log_path/mv_log
done
done
exit
for f in $data_path/Match/*.zip; do	
	filename=`print $f | sed 's:.*/::'`
	unzip -xu -d$data_path/Match/unzipped $f >>$log_path/unzip.log 2>> $log_path/unzipErrors.log
	if [ "$?" -eq 0 ]
		then 
			##rm $f
			mysql -h$HOST -u$USER -p$PW $DB -e "INSERT IGNORE INTO import_log VALUE ('$filename', CURRENT_TIMESTAMP())"
				
	fi
done



if [ "$?" -eq 0 ]
	then
	print "errors detected in match file creation"
		#rm -f $data_path/Match/unzipped/*.csv
fi
print "ftpImport.sh complete"
