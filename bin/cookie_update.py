#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import sys
import os
import re
import datetime
from warnings import filterwarnings
print "beginning update"
filterwarnings('ignore', category = mdb.Warning)

#os.system("bash /usr/local/ftp_sync/bin/ftpImport.sh")

#os.system("python /usr/local/ftp_sync/bin/match.py")

host = 'localhost'
user = 'tomb'
pw = 'DW4mediatb'
db = 'DWA_SF_Cookie'

log_path="/usr/local/ftp_sync/logs"

# get db handle:
try:
	con = mdb.connect(host, user, pw, db)
	cur = con.cursor()

	con.set_character_set('utf8')
	cur.execute('SET NAMES utf8;') 
	cur.execute('SET CHARACTER SET utf8;')
	cur.execute('SET character_set_connection=utf8;')

        
except mdb.Error, e:

	print "Error %d: %s" % (e.args[0], e.args[1])
	sys.exit(1)

cur.execute("SELECT AdvertiserName, AdvertiserID FROM SF_Match.Advertisers")
advert = cur.fetchall()
for a in advert:
	print a[0], a[1]
	tableName0=a[0].replace(" ", "_")+"_Std"
	tableName="DWA_SF_Cookie." +  tableName0
	# check if table exists:
	cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%s'"%tableName0);
	res = cur.fetchall()
	if len(res) == 0:
		print "Creating table %s"%tableName
		stmt1 = "CREATE TABLE IF NOT EXISTS %s"%tableName + " AS SELECT UserID, EventID, EventTypeID, STR_TO_DATE(EventDate, '%m/%d/%Y %h:%i:%s %p') AS EventDate, CampaignID, SiteID, PlacementID, IP, AdvertiserID FROM DWA_SF_Cookie.MM_Standard_tmp WHERE 1=0"
		stmt2 = "ALTER TABLE %s ADD PRIMARY KEY(EventID)"%tableName
		stmt3 = "ALTER TABLE %s ADD INDEX (UserID)"%tableName
		stmt4 = "ALTER TABLE %s ADD INDEX (EventDate)"%tableName
	
		cur.execute(stmt1)
		cur.execute(stmt2)
		cur.execute(stmt3)
		cur.execute(stmt4)

	print "updating %s"%tableName	
	stmt5 = "INSERT IGNORE INTO %s"%tableName + " SELECT UserID, EventID, EventTypeID, STR_TO_DATE(EventDate, '%m/%d/%Y %h:%i:%s %p'), CampaignID, SiteID, PlacementID, IP, AdvertiserID FROM DWA_SF_Cookie.MM_Standard_tmp WHERE AdvertiserID ="+" %s"%a[1]
	print stmt5
	os.system("echo %s update begun at %s>> %s/adTables.log"%(tableName, datetime.datetime.now(), log_path))
	cur.execute(stmt5)
	os.system("echo %s update completed at %s>> %s/adTables.log"%(tableName, datetime.datetime.now(), log_path))
	
cur.execute("INSERT INTO MM_Standard SELECT * FROM MM_Standard_tmp")
#cur.execute("TRUNCATE  MM_Standard_tmp")	







if con:
	con.close()


