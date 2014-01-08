#!/usr/bin/python

import _mysql
import sys
import os
import re

data_path="../downloaded/"
match_path = data_path + "Match/"


for f in os.listdir(match_path):
	if re.search("^MM", f):
		tableName = re.sub("MM_CLD_Match_", "", f)
		print "intermediate = ", tableName
		tableName = re.sub("Match[fF]ile.*", "", tableName) # Mediamind conveniently capitalizes its F's unpredictably

		header = next(open(match_path + f))
		print header.split(" ")


		
		print tableName, "\n\n"#, " filename = ", f










try:
    con = _mysql.connect('184.105.184.30', 'tomb', 'DW4mediatb', 'DWA_SF_Cookie')
        
except _mysql.Error, e:
  
    print "Error %d: %s" % (e.args[0], e.args[1])
    sys.exit(1)

finally:
    if con:
        con.close()

