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



def ftp_sync(sync_dir):
	p1 = subprocess.Popen(['echo', "nlist" ], stdout= subprocess.PIPE)
	server_files = subprocess.check_output(["ftp", "-p", "-i", "ftp.platform.mediamind.com"], stdin = p1.stdout).split()
	local_files = subprocess.check_output(["ls", sync_dir]).split()
	cmd = ""
	for sf in server_files:
		if sf not in local_files and "zip" in sf:
			cmd += "get %s\n"%sf
	cmd = "'" + cmd + "'"
	p1 = subprocess.Popen(['echo', cmd], stdout=subprocess.PIPE)
	p2 = subprocess.call(["ftp", "-p", "-i", "ftp.platform.mediamind.com"], stdin = p1.stdout)
	return
		
	
def partition_by_day(tblName,cur, startDate = -90, endDate = 30):
	cur.execute("USE DWA_SF_Cookie")
	# ugly one liner to get list of dates
	days = [str(datetime.date.today() + datetime.timedelta(days=x)) for x in range(-90,30)]


	dates = []
	stmt = "ALTER TABLE %s PARTITION BY RANGE Columns (EventDate) ("%tblName
	for d in days:
		stmt += "PARTITION `p%s` VALUES LESS THAN ('%s'), "%(d,d)
	stmt += "PARTITION pMAX VALUES LESS THAN (MAXVALUE))"
	cur.execute(stmt)
		
	
	
	
def csv_Standard(file_name, ad_dict, cur,con, insert_interval = 1, print_interval = 1000):
	cur.execute("USE DWA_SF_Cookie")
	# distribute Standard CSV file into DB tables for each Advertiser.

	# initialize. col_names_set is variable to tell if we have loaded column names yet.
	# line_i and start are vars for benchmarking.
	 
	con.autocommit(False)
	col_names_set = False
	line_i = 0
	overall_start = datetime.datetime.now()
	start = datetime.datetime.now()
	#print "inserting from %s, printing times for insertion of %s records"%(file_name,insert_interval)
	
	# make a dict for insert statements. Each table keeps a running record of how many 
	# records are currently loaded, along with the statment itself. If insert_interval
	# is reached, stmt is executed.
	
	insert_d = {}
	for id, name in ad_dict.iteritems():
		insert_d[id] = {"tblName":name,"stmt":"","records":0}

	for l in open(file_name,"r"):

		vals = [i.replace("\r\n","") for i in l.split(",")]	

		if not col_names_set:
			col_names = vals
			col_names_set = True
		else:
			# with keys set, make a dict of the line. A few ugly lines for URL/date weirdness.
			row_d = {}
			for i in range(len(vals)):
				row_d[col_names[i]] = "'%s'"%vals[i]
			# urls may contain ' chars. 
			row_d["Referrer"] = "'%s'"%row_d["Referrer"].replace("'","")
			row_d['EventDate'] = "STR_TO_DATE(%s,'%%c/%%e/%%Y %%l:%%i:%%s %%p')"%row_d['EventDate']
			# remove query string from referrer - hunt for ? or ; chars and truncate string there.

			for i in range(len(row_d["Referrer"])):
				if row_d["Referrer"][i] in ["?",";"]:
					row_d["Referrer"] = row_d["Referrer"][:i] + "'"
					break

			# with values loaded, detect which advertiser we have. Some unfortunate string
			# to int handling is necessary.
			adID = int(row_d["AdvertiserID"].replace("'",""))
			stmt = insert_d[adID]["stmt"]
	
			# if statement is empty, initialize. Else just add to it.
			if stmt == "": 
				stmt = "INSERT IGNORE INTO %s "%insert_d[adID]["tblName"] + \
				"(UserID, EventID, EventTypeID, EventDate, CampaignID, SiteID," + \
				" PlacementID, IP, AdvertiserID,Referrer) VALUES "
				
			stmt_add = "(" + "%s,"*9 + "%s),"
			stmt_add = stmt_add%(row_d['UserID'],row_d['EventID'], row_d['EventTypeID'], 
			row_d['EventDate'], row_d['CampaignID'],row_d['SiteID'], 
			row_d['PlacementID'], row_d['IP'], row_d['AdvertiserID'], row_d["Referrer"])
			stmt += stmt_add	
	
			# increment records, check if execution is needed.
			records = insert_d[adID]["records"] + 1
			if records == insert_interval:
				stmt = stmt[:-1] # remove trailing comma

				# benchmarking:
				time_elapsed = (datetime.datetime.now()-overall_start).seconds + 1 # avoid div by zero
				records_per_second = line_i/time_elapsed
				print "\tinserting %s records into %s: time: %s, %s records per sec"%(
				records,insert_d[adID]["tblName"],time_elapsed, records_per_second)
				
				# reset
				cur.execute(stmt)
				stmt, records = "",0
				
			insert_d[adID]["stmt"], insert_d[adID]["records"] = stmt, records
			line_i += 1
				
	# done looping, execute all remaining statements.
	for adID, vals in insert_d.iteritems():
		stmt = vals["stmt"]
		if stmt != "":
			stmt = stmt[:-1]
			cur.execute(stmt)	
	return
	
	

def create_ad_tables(cur, drop=False):
	ad_dict = get_ad_dict(cur)
	cur.execute("USE DWA_SF_Cookie")
	for k, tblName in ad_dict.iteritems():
		if drop:
			cur.execute("DROP TABLE IF EXISTS %s"%tblName)
		stmt = "CREATE TABLE IF NOT EXISTS %s ("%tblName + \
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
		
  
def unzip_all(zip_dir, unzip_dir, fileType, cur, add_to_exclude=True):
        cur.execute("SELECT filename FROM exclude_list")
        excludes = [x[0] for x in cur.fetchall()]
	files = subprocess.check_output(["ls", zip_dir]).split()
	for f in files:
	#	print f
		if f not in excludes and fileType in f:
			ret = subprocess.call(["unzip", "-u", zip_dir+f, "-d", unzip_dir])
			if ret == 0:
				cur.execute("INSERT IGNORE INTO exclude_list VALUES ('%s')"%f)

def load_all_Standard(files_dir,cur,con,insert_interval = 1000):
	files = subprocess.check_output(["ls", files_dir]).split()	
	cur.execute("SELECT filename FROM DWA_SF_Cookie.exclude_list")	
	excludes = [x[0] for x in cur.fetchall()]	


	ad_dict = get_ad_dict(cur)

	for f in files:
		if "Standard" in f and f not in excludes:
			ret = csv_Standard(files_dir + f, ad_dict,cur,con,insert_interval)
			if ret is None:
				cur.execute("INSERT IGNORE INTO exclude_list VALUES ('%s')"%f)
				con.commit()			
	
def main():
	con,cur = mysql_login.mysql_login()
	con.autocommit(False)
#	unzip_all("/usr/local/var/ftp_sync/downloaded/", "/usr/local/var/ftp_sync/downloaded/Standard/","Standard", cur)
	cur.execute("USE DWA_SF_Cookie")
	cur.execute("SHOW TABLES LIKE 'Std%'")
	tbls = [t[0] for t in cur.fetchall()]
	for t in tbls:
		partition_by_day(t,cur)
	con.commit()		
	return

	create_ad_tables(cur, True)	
	files_dir = "/usr/local/var/ftp_sync/downloaded/Standard/"
	load_all_Standard(files_dir,cur,con, 1000)
	end = datetime.datetime.now()
	print "inserting %s records at a time took %s seconds"%(iv, (end-start).seconds)
		
	#create_ad_tables(cur,True)


	#ftp_sync("/usr/local/var/ftp_sync/downloaded")
	if con:
		con.commit()
		con.close()		
main()
