#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import sys
import os
import re
from warnings import filterwarnings

filterwarnings('ignore', category = mdb.Warning)

host = '184.105.184.30'
user = 'tomb'
pw = 'DW4mediatb'
db = 'SF_Match'

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

cur.execute("SELECT AdvertiserName, AdvertiserID FROM Advertisers")
advert = cur.fetchall()
for a in advert:
	tableName = a[0].replace(" ", "_")+"_Standard"
	# check if table exists:
	cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%s'"%tableName);
	res = cur.fetchall()

	if len(res) == 0:
		print "asdf"
#		stmt0 = "DROP TABLE IF EXISTS MM_Standard_P.%s"%tableName
		stmt1 = "CREATE TABLE IF NOT EXISTS MM_Standard_P.%s AS SELECT * FROM DWA_SF_Cookie.MM_Standard_P WHERE 1=0"%tableName
		stmt2 = "ALTER TABLE MM_Standard_P.%s ADD PRIMARY KEY(EventID)"%tableName
		stmt3 = "ALTER TABLE MM_Standard_P.%s ADD INDEX (UserID)"%tableName
		stmt4 = "ALTER TABLE MM_Standard_P.%s ADD INDEX (EventDate)"%tableName
	#	cur.execute(stmt0)
		cur.execute(stmt1)
		cur.execute(stmt2)
		cur.execute(stmt3)
		cur.execute(stmt4)
	
	stmt5 = "INSERT IGNORE INTO MM_Standard_P.%s SELECT UserID, EventID, EventTypeID, EventDate, CampaignID, SiteID, PlacementID, IP, AdvertiserID FROM DWA_SF_Cookie.MM_Standard_P WHERE AdvertiserID = %s"%(tableName, a[1])


	cur.execute(stmt5)



if con:
	con.close()

