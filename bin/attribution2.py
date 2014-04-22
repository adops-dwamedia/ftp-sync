#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import subprocess
import sys
import os
import re
import datetime
import cookie_update
import mysql_login
from warnings import filterwarnings
filterwarnings('ignore', category = mdb.Warning)

def get_ad_dict(cur):
	cur.execute("SELECT AdvertiserName, AdvertiserID FROM SF_Match.Advertisers")
	ad_dict = {}
	for n, adid in cur.fetchall():
		ad_dict[adid] = "DWA_SF_Cookie.Std_"+n.replace(" ","_")
	return ad_dict

def initialize(cur,con):
	cur.execute("CREATE DATABASE IF NOT EXISTS attribution")
	cur.execute("USE attribution")
	models = ["last_imp", "first_imp"]

	stmt = "CREATE TABLE IF NOT EXISTS attribution (convID CHAR(36), modelID TINYINT,  eventID CHAR(36), value FLOAT, PRIMARY KEY(convID, modelID, eventID))"
	cur.execute(stmt)
	stmt = "CREATE TABLE IF NOT EXISTS MODELS(modelName VARCHAR(255), modelID TINYINT, PRIMARY KEY (modelID))"
	cur.execute(stmt)
		
# data gatherers		
def get_events(convID, convDate, adID, userID, cur,con):
	ad_dict = get_ad_dict(cur)


	conv_d = {"conversionID":convID, "conversionDate":convDate, "advertiserID":adID, "userID": userID, "events" : []}
	# each event will have id, eventType, date, and campaignID.
	
	# imps and clicks. 
	cur.execute("SELECT eventID, eventTypeID, eventDate, campaignID FROM %s WHERE"%ad_dict[adID] +\
	" userID = '%s' AND "%userID +\
	" advertiserID = %s AND "%adID +\
	" eventDate < '%s'"%convDate)
	events =list(cur.fetchall())
	
	# other conversions
	cur.execute("SELECT conversionID, 8, conversionDate, NULL FROM DWA_SF_Cookie.MM_Conversion WHERE " +\
	" userID = '%s' AND "%userID +\
	" advertiserID = %s AND "%adID +\
	" conversionDate < '%s'"%convDate)
	events += list(cur.fetchall())

	for eventID, eventTypeID, eventDate, campaignID in events:
		conv_d['events'] += [{'eventID':eventID, 'eventTypeID':eventTypeID, 'eventDate':eventDate, 'campaignID':campaignID}]
		
	return conv_d
	
def get_conversions(cur,con,exclude_tblName = None):
	""" gathers conversions not already in tblName """
	stmt = "SELECT conversionID, userID, conversionDate, AdvertiserID " +\
	"FROM DWA_SF_Cookie.MM_Conversion" 
	
	cur.execute(stmt)
	return cur.fetchall()

# pruning functions
def prune_by_window(conv_d, max_window):
	""" prunes events older than max_window days, relative to conversion. Returns dict. """
	# ugly oneliner to filer dict
	conv_d['events'] = [e for e in conv_d['events'] if (conv_d['conversionDate']-e['eventDate']).days <= max_window]
	return conv_d
def prune_by_eventType(conv_d,eventType):
	""" prunes events by event type(s). If multiple types are to be included, accepts a list """ 
	if type(eventType) is int: eventType = [eventType] 
	
	conv_d['events'] = [e for e in conv_d['events'] if e['eventTypeID'] not in eventType]
	return conv_d
	
# model appliers. All assume relatively well formed objects, and return a list of lists: [id1, val1], [id2, val2]
def last_imp(d):
	winner = min(d['events'], key=lambda e: d['conversionDate'] - e['eventDate'])
	return 	[[winner['eventID'],1]]
		 
def calculate_all(cur,con, conversion_window = 30):
	cur.execute("USE attribution")
	# gather global functions
	global last_imp

	models = {
		"last_imp":{
			"function":last_imp, 
			"modelID":1,
			}
		}
	conversions = get_conversions(cur,con)	
	ad_dict = get_ad_dict(cur)
	base_insert_stmt = "INSERT IGNORE INTO attribution (convID, modelID, eventID, value) VALUES (%s,%s,%s,%s)"
	records = []
	numb_records = len(conversions)
	numb_processed = 0
	for convID, userID, convDate, AdID in conversions:
		d = get_events(convID, convDate, AdID, userID, cur,con)
		d = prune_by_window(d, 30)
		if len(d['events']) == 0: continue
		for k,v_d in models.iteritems():
			for eID, val in v_d['function'](d):
				records.append((convID, v_d['modelID'],eID,val))
		if len(records) > 100:
			numb_processed += len(records)
			cur.executemany(base_insert_stmt, records)
			records = []
			print "%s processed, %s%%"%(numb_processed, round(numb_processed*100.0/numb_records))
	


	

	

def main():
        start = datetime.datetime.now()
        con,cur = mysql_login.mysql_login()
        con.autocommit(False)
	initialize(cur,con)
	calculate_all(cur,con)




        if con:
                con.commit()
                con.close()

