#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import sys
import os
import re
from warnings import filterwarnings

# Partition maintenance file. Partition field and type are hard coded. Checks if M_Standard_P table is partitioned. 
#If it is, make sure all unique values of AdvertiserID are included. IF not, partition table


filterwarnings('ignore', category = mdb.Warning)

host = 'localhost'
user = 'tomb'
pw = 'DW4mediatb'
db = 'DWA_SF_Cookie'


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


cur.execute("SELECT advertiserID FROM SF_Match.Advertisers")
advertisers_raw = cur.fetchall()

advert_ls = []
for ad in advertisers_raw:
	advert_ls.append(ad[0])

cur.execute("EXPLAIN PARTITIONS SELECT * FROM MM_Standard_P");

res = cur.fetchall()
#print res
partitions = res[0][3]
parts_ls = []
#print "current partitions = ", partitions
#print advert_ls
if partitions is not None:
	for p in partitions.split(","):
		parts_ls.append(p.replace("p",""))


	for ad in advert_ls:
		if ad not in parts_ls:
			stmt = "ALTER TABLE MM_Standard_P ADD PARTITION (PARTITION p%s VALUES IN (%s))"%(ad,ad)
			cur.execute(stmt)
else:
	stmt = "ALTER TABLE MM_Standard_P PARTITION BY LIST COLUMNS (AdvertiserID)("
	for ad in advert_ls:
		stmt += "PARTITION p%s VALUES IN (%s),"%(ad,ad)
	stmt = stmt[:-1] + ")"
	#print stmt
	cur.execute(stmt)

if con:
	con.close()

