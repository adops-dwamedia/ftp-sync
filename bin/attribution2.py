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

def initialize(cur,con, verbose=False):
	if verbose: print "\tinitializing attribution db. . . "
	cur.execute("CREATE DATABASE IF NOT EXISTS attribution")
	cur.execute("USE attribution")
	models = [
		{"name":"last_imp","function":last_imp, "id":1}, 
		{"name":"first_imp","function":first_imp, "id":2},
		{"name":"equal_imp", "function":equal_imp, "id":3}
		]

	stmt = "CREATE TABLE IF NOT EXISTS attribution (convID CHAR(36), modelID TINYINT,  winnerEventID CHAR(36), value FLOAT, description VARCHAR(255), PRIMARY KEY(convID, modelID, winnerEventID))"
	cur.execute(stmt)
	stmt = "CREATE TABLE IF NOT EXISTS models(modelName VARCHAR(255), modelID TINYINT, UNIQUE KEY (modelID))"
	cur.execute(stmt)
	
	for m in models:
		stmt = "INSERT IGNORE INTO models (modelName, modelID) VALUES ('%s',%s)"%(m["name"], m["id"])
		cur.execute(stmt)
"""
convert model into a class.

class model
	id
	name
	pruning function
	winner calculating function
"""



	
# data gatherers	
def reset(cur,con):
	cur.execute("DROP DATABASE IF EXISTS attribution")
		
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
	
def get_conversions(cur,con,exclude_tblName = None, debug = False):
	""" gathers conversions not already in tblName """
	stmt = "SELECT conversionID, userID, conversionDate, AdvertiserID " +\
	"FROM DWA_SF_Cookie.MM_Conversion" 

	if debug:
		stmt += " ORDER BY conversionDate DESC LIMIT 100"
	
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
def first_imp(d):
	winner = max(d['events'], key=lambda e: d['conversionDate'] - e['eventDate'])
	return 	[[winner['eventID'],1]]
def equal_imp(d):
	vals = 1.0/len(d['events'])
	winners = []
	for w in d['events']:
		winners.append([w['eventID'], vals])
	return winners	

		 
def calculate_all(cur,con, conversion_window = 30, debug=False, verbose=False):
	cur.execute("USE attribution")
	# gather global functions
	global last_imp
	global first_imp

	# to be defined upstream from here
	models = {
		"last_imp":{
			"function":last_imp, 
			"modelID":1,
			},
		"first_imp":{
			"function":first_imp,
			"modelID":2,
			},
		"equal_imp":{
			"function":equal_imp,
			"modelID":3,
			}
		}
	if verbose: print "\t Gathering conversion data. . ."
	conversions = get_conversions(cur,con, debug=debug)
	if verbose: print "\t Conversion data gathered.	"
	ad_dict = get_ad_dict(cur)
	base_insert_stmt = "INSERT IGNORE INTO attribution (convID, modelID, winnerEventID, value) VALUES (%s,%s,%s,%s)"
	records = []
	numb_records = len(conversions)
	numb_processed = 0
	print_threshold = .1	

	for convID, userID, convDate, AdID in conversions:
		d = get_events(convID, convDate, AdID, userID, cur,con)
		d = prune_by_window(d, 30)
		if len(d['events']) == 0: continue # if no events for conversion, move on
		for k,v_d in models.iteritems():
			for eID, val in v_d['function'](d):
				records.append((convID, v_d['modelID'],eID,val))
		if len(records) > 100:
			cur.executemany(base_insert_stmt, records)
			records = []
		numb_processed += 1
		if numb_processed*1.0/numb_records > print_threshold:
			print "\t%s records processed, %s%%"%(numb_processed, numb_processed*100.0/numb_records)
			print_threshold += .1	
	
	if verbose: print "\tcleaning up. . ."
	if len(records) > 0:	cur.executemany(base_insert_stmt,records)
	

	

def main():
        start = datetime.datetime.now()
        con,cur = mysql_login.mysql_login()
	reset(cur,con)
        con.autocommit(False)
	cur.execute("USE DWA_SF_Cookie")
        cur.execute("SHOW TABLES LIKE '%%Std'") 
        stmt = "CREATE VIEW MM_Standard AS "
        for tbl in [res for res[0] in results if res[0] != "MM_Standard"]:
                stmt += "SELECT * FROM %s UNION ALL "%tbl
        stmt = stmt[:-10]
        cur.execute(stmt)

	initialize(cur,con, verbose=True)
	calculate_all(cur,con, debug=True, verbose=True)




        if con:
                con.commit()
                con.close()
main()
