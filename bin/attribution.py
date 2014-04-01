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

def get_ad_dict(cur):
	cur.execute("SELECT AdvertiserName, AdvertiserID FROM SF_Match.Advertisers")
	ad_dict = {}
	for n, adid in cur.fetchall():
		ad_dict[adid] = "Std_"+n.replace(" ","_")
	return ad_dict

def initialize(cur,con):
	cur.execute("CREATE DATABASE IF NOT EXISTS attribution")
	cur.execute("USE attribution")
	models = ["last_imp", "first_imp"]

	for m in models:
		stmt = "CREATE TABLE IF NOT EXISTS %s (convID CHAR(36), convDate DATETIME, "%m +\
		"eventID CHAR(36), value FLOAT,"
		stmt += "PRIMARY KEY (convID, convDate))"
		
		cur.execute(stmt)
def gather_events(convID, convDate, adID, userID, cur,con):
	ad_dict = get_ad_dict(cur)


	conv_d = {"conversionID":convID, "conversionDate":convDate, "advertiserID":adID, "userID": userID, "events" = []}
	# each event will have id, eventType, date, and campaignID.
	
	# imps and clicks. 
	cur.execute("SELECT eventID, eventTypeID, eventDate, campaignID FROM %s WHERE"%ad_dict[adID] +\
	" userID = '%s' AND "%userID +\
	" advertiserID = %s AND "%adID +\
	" eventDate < '%s'"%convDate
	events =list(cur.fetchall())
	
	# other conversions
	cur.execute("SELECT conversionID, 8, conversionDate, NULL FROM MM_Conversion WHERE " +\
	" userID = '%s' AND "%userID +\
	" advertiserID = %s AND "%adID +\
	" conversionDate < '%s'"%conversionDate
	events += list(cur.fetchall())

	for eventID, eventTypeID, eventDate, campaignID in events:
		conv_d['events'] += [{'eventID':eventID, 'eventTypeID':eventTypeID, 'eventDate':eventDate, 'campaignID':campaignID}]
		
		
	print conv_d
	
	
	
	
	
def last_imp(cur,con):
	print "calculating last imp conversions..."
	ad_dict = get_ad_dict(cur)
	sys.stdout.write( "\tgathering conversions. . ." )
	cur.execute("SELECT conversionID, userID, conversionDate, AdvertiserID FROM DWA_SF_Cookie.MM_Conversion WHERE conversionID NOT IN (SELECT convID FROM last_imp)")

	convs = cur.fetchall()
	total_records = len(convs)
	print " %s found."%total_records
	base_insert_stmt = "INSERT IGNORE INTO last_imp (convID, convDate, eventID, value) VALUES "
	insert_stmt = base_insert_stmt
	records = 0
	# get last imp.
	for convID, userID, convDate, AdID in convs:
		d = gather_events(convID, convDate, AdID, userID, cur,con):
		return
		
		tblName = ad_dict[AdID]
		stmt = "SELECT eventID FROM DWA_SF_Cookie.%s WHERE userID = '%s' AND AdvertiserID = '%s' AND EventDate < '%s' AND eventTypeID = 1 ORDER BY eventDate LIMIT 1"%(tblName, userID, AdID, convDate)
		cur.execute(stmt)
		
		winner_eventID = cur.fetchall()
		
		if len(winner_eventID) > 0:
			winner_eventID = winner_eventID[0][0]
			insert_stmt += "('%s', '%s', '%s', 1),"%(convID, convDate, winner_eventID)
			records += 1
			if records%100 == 0:
				
				cur.execute(insert_stmt[:-1])
				insert_stmt = base_insert_stmt
				print "\t\t%s of %s conversions processed, %s percent"%(records,total_records, int(100.0*records/total_records))
				con.commit()
	if records > 0:
		cur.execute(insert_stmt[:-1])
	


	

	

def main():
        start = datetime.datetime.now()
        con,cur = mysql_login.mysql_login()
        con.autocommit(False)
	initialize(cur,con)
	last_imp(cur,con)




        if con:
                con.commit()
                con.close()
main()
