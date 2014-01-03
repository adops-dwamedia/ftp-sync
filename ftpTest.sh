#!/usr/bin/env bash

SQL_PATH="./SQL/"
DATA_PATH="./downloaded/"
LOG_PATH="./logs/"
TMP_PATH="./tmp/"

# first, retrieve list of available files from server. 
ftp -pivd ftp.platform.mediamind.com > transfer.log << EOF
nlist . oFile
quit
EOF

# strip out extra formatting
cat oFile | grep MM_CLD_RICH.*131205.*\.zip > files.txt
rm oFile

rm -f toDownload
touch toDownload

rm -f downloadCmds
touch downloadCmds
while read l; do
	# if filename is not in .downloaded, add to list of files to retrieve
	if [ -z `cat .downloaded | grep $l` ] 
	then
		echo "get $l ./downloaded/$l" >> downloadCmds
		echo $l >> toDownload
	fi
	
done < files.txt

# retrieve files. 
ftp -vpi ftp.platform.mediamind.com < downloadCmds 2> ftpErrors.log

if [[ `cat ftpErrors.log` != "" ]]
then
	echo "problemos"
	exit 
fi

rm downloadCmds
cat toDownload >> .downloaded


# Import downloaded files to database. Separate Rich, Standard, and Conversion 
mv *Rich* ./downloaded/Rich
mv *Standard* ./downloaded/Standard
mv *Conversion* ./downloaded/Conversion











