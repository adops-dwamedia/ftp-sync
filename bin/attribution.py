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

	for m in models:
		tblName = "attribution.%s"%m
		stmt = "CREATE TABLE IF NOT EXISTS %s (convID CHAR(36), convDate DATETIME, "%tblName +\
		"eventID CHAR(36), value FLOAT,"
		stmt += "PRIMARY KEY (convID, convDate))"
		
		cookie_update.partition_by_day(tblName,cur,col="convDate",startDate = -90, endDate = 30)
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
	
	if exclude_tblName is not None:
		stmt += " WHERE conversionID NOT IN (SELECT convID FROM %s)"%exclude_tblName
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
		 
def insert_smt(cur,con,insert_stmt, base_stmt, records, total_records, insert_interval = 100, verbose = True):
	if records%insert_interval == 0:
		cur.execute(stmt)
		print "\t\t%s of %s records processed, %s percent"%(records, total_records, records*100.0/total_records)
		return base_stmt,records + 1
	else:
		return stmt, records +1

def calculate_all(cur,con, conversion_window = 30):
	cur.execute("USE attribution")
	# gather global functions
	global last_imp

	models = {
		"last_imp":{
			"function":last_imp, 
			"base_insert_stmt": "INSERT IGNORE INTO last_imp (convID, convDate, eventID, value) VALUES "
			}
		}
	for k in models.keys():
		models[k]['insert_stmt'] = models[k]['base_insert_stmt']
		models[k]['conversions'] = get_conversions(cur,con, k)
		models[k]['total_records'] = len(models[k]['conversions'])
		models[k]['records'] = 0
			
	ad_dict = get_ad_dict(cur)
	for k,v_d in models.iteritems():
		for convID, userID, convDate, AdID in v_d['conversions']:
			d = get_events(convID, convDate, AdID, userID, cur,con)
			d = prune_by_window(d, 30)
			d = prune_by_eventType(d,1)
			if len(d['events']) == 0: continue
		
			for eID, val in v_d['function'](d):
				v_d['insert_stmt'] += "('%s','%s','%s','%s'),"%(convID, convDate,eID,val)
				v_d['records'] += 1
				if v_d['records']%100 == 0:
					cur.execute(v_d['insert_stmt'][:-1])
					v_d['insert_stmt'] = v_d['base_insert_stmt']
					print "\t\t%s of %s conversions processed, %s percent"%(v_d['records'],v_d['total_records'], int(100.0*v_d['records']/v_d['total_records']))
					
		if v_d['insert_stmt'][-7:] != "VALUES " > 0:
			cur.execute(v_d['insert_stmt'][:-1])
	


	

	

def main():
        start = datetime.datetime.now()
        con,cur = mysql_login.mysql_login()
        con.autocommit(False)
	initialize(cur,con)
	calculate_all(cur,con)




        if con:
                con.commit()
                con.close()
