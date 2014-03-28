#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import subprocess
import sys
import os
import re
import datetime
import mysql_login
from warnings import filterwarnings
filterwarnings('ignore', category = mdb.Warning)



def initialize(cur,con):
	print "\tchecking for missing tables..."
	
	# gather list of Std tables, gather list of conversion tables. If a conversion table 
	# is missing, create it.
	
	cur.execute("USE DWA_SF_Cookie")
	cur.execute("SHOW TABLES LIKE 'Std%'")
	std_tbls = [t[0] for t in cur.fetchall()]
	
	conv_tblNames = [t.replace("Std_", "Std_Conv_") for t in std_tbls]
	
	cur.execute("USE Std_FilterImps")
	cur.execute("SHOW TABLES")
	std_conv_tbls = [t[0] for t in cur.fetchall()]
	
	for ctn in conv_tblNames:
		if ctn not in std_conv_tbls:
			print "\t\tCreating table %s"%ctn
			stmt = "CREATE TABLE IF NOT EXISTS Std_FilterImps.%s ("%ctn + \
			"UserID char(36) NOT NULL," +\
			"EventID char(36) NOT NULL," +\
			"EventTypeID tinyint(4) NOT NULL," +\
			"EventDate datetime NOT NULL," +\
			"CampaignID mediumint(9) NOT NULL," +\
			"SiteID int(11) NOT NULL DEFAULT 0," +\
			"PlacementID int(11) NOT NULL DEFAULT 0," +\
			"IP varchar(16) NOT NULL DEFAULT ''," +\
			"AdvertiserID mediumint(9) NOT NULL DEFAULT 0,"+\
			"Referrer varchar(255) NOT NULL DEFAULT '',"+\
			"PRIMARY KEY (EventID, EventDate)," +\
			"KEY userID (userID, EventDate)," +\
			"KEY eventDate (eventDate))" 
			cur.execute(stmt)
	#create table to be used later for excluding conversions from processing
	cur.execute("CREATE TABLE IF NOT EXISTS converting_users(userID char(36) PRIMARY KEY, lastUpdate DATETIME, INDEX (lastUpdate))")
	con.commit()
	print "\tdone."
def update(cur,con, conv_interval=30, insert_interval = 1000):
	print "\tupdating data for tables..."
	# create a dict. key is AdvertiserID, include:
	# 	Std table name
	#	Std_Conv table name
	#	list lists: userID and timestamp of conversions.
	# conv_interval is the max length of time an imps can influence a conversion.
	ad_dict = {}
	
	# get list of users 
	cur.execute("SELECT AdvertiserID, AdvertiserName FROM SF_Match.Advertisers")
	for id, adName in cur.fetchall():
		std_tblName = "Std_%s"%adName.replace(" ", "_")
		conv_tblName = std_tblName.replace("Std_", "Std_Conv_")
		

		ad_dict[id] = {
			'std_tblName':std_tblName,
			'conv_tblName':conv_tblName
			}
		
		# get new users and users that need to be updated. Two queries, one for returning
		# converters and one for new converters.
		
		# new converters
		stmt = "SELECT userID, max(conversionDate) FROM DWA_SF_Cookie.MM_Conversion " +\
		"WHERE AdvertiserID = %s AND "%id +\
		"userID NOT IN (SELECT userID FROM converting_users) GROUP BY userID"
		#print stmt
		cur.execute(stmt)
		user_rows = list(cur.fetchall())
#		if user_rows[0] == (None,None):
#			user_rows.pop(0)
		new_users = len(user_rows)
		
		# returning converters
		stmt = "SELECT a.userID, MAX(a.conversionDate) FROM DWA_SF_Cookie.MM_Conversion a JOIN "+\
		"converting_users b ON a.userID = b.userID WHERE a.conversionDate > b.lastUpdate AND "+\
		"AdvertiserID = %s GROUP BY (a.userID)"%id 
		#print stmt
		cur.execute(stmt)
		returning_users = list(cur.fetchall())
		if len(returning_users) > 0:
			user_rows += list(cur.fetchall())

		print "\t\tUpdating %s: "%conv_tblName +\
		"%s new converters, %s returning converters"%(new_users, len(user_rows) - new_users)

		
		to_insert = []
		i = 0
		print_interval = 100
		total = len(user_rows)
		insert_Stmt = "INSERT IGNORE INTO Std_FilterImps.%s VALUES ("%conv_tblName +\
		"%s,"*9 + "%s)"
		
		# eventIDs already inserted
		cur.execute("SELECT EventID FROM Std_FilterImps.%s"%conv_tblName)
		exclude_eventID = [t[0] for t in cur.fetchall()]
		
		for ur in user_rows:
			i += 1
			if i%print_interval ==0:
				print "\t\t\t%s of %s records, %s%% completed."%(i,total, int(i*100.0/total))
			
			stmt = "SELECT * FROM DWA_SF_Cookie.%s "%std_tblName +\
			"WHERE userID = '%s' AND "%ur[0] +\
			"EventDate < '%s' AND "%ur[1] +\
			"EventDate > DATE_ADD('%s', INTERVAL -%s DAY)"%(ur[1], conv_interval)
			cur.execute(stmt)
			to_insert += cur.fetchall()	
			
			if len(to_insert) > 1000:
				# filter eventID's already inserted
				to_insert = [ti for ti in to_insert if ti[1] not in exclude_eventID]
			
			
			while len(to_insert) > insert_interval:
				cur.executemany(insert_Stmt, to_insert[:1000])
				to_insert = to_insert[1000:]
			con.commit()
			stmt = "INSERT INTO converting_users VALUES ('%s','%s')"%(ur[0], ur[1]) +\
			"ON DUPLICATE KEY update lastUpdate = '%s'"%ur[1]
#			print stmt
			cur.execute(stmt)				
			con.commit()

		while len(to_insert) > insert_interval:
			cur.executemany(insert_Stmt, to_insert[:1000])
			to_insert = to_insert[1000:]
				
		if len(to_insert) > 0:
			cur.executemany(insert_Stmt, to_insert)
		con.commit()
	print "\tdone."
	
	
	
	
	
	
	
	
	
	
	





def main():
	print "\nUpdating Std_FilterImps db..."
	# create conversion tables if they do not exist.
	# for each tbl, gather userID's of users who have converted or clicked
	# insert ignore records of each userID into Std_Conversion table.
	start = datetime.datetime.now()
	con,cur = mysql_login.mysql_login()
	con.autocommit(False)
	
	initialize(cur,con)
	update(cur,con)
main()
