#!/usr/bin/env bash

# first, retrieve list of available files from server. 
ftp -pivd ftp.platform.mediamind.com > transfer.log << EOF
nlist . oFile
quit
EOF

# strip out extra formatting
cat oFile | grep MM.*131205.*\.zip > files.txt
rm oFile

touch toDownload

while read l; do
	# if filename is not in .downloaded, add to list of files to retrieve
	if [ -z `cat .downloaded | grep $l` ] 
	then
		echo "get $l ./downloaded/$l" >> toDownload
	fi
	
done < files.txt

# retrieve files. 
ftp -vpi ftp.platform.mediamind.com < toDownload 1> regular.log
rm toDownload
# Import downloaded files to database. Separate Rich, Standard, and Conversion 

mv *Rich* ./downloaded/rich
mv *Standard* ./downloaded/Standard
mv *Conversion* ./downloaded/conversion











