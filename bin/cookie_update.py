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



def ftp_sync(sync_dir):
	p1 = subprocess.Popen(['echo', "nlist" ], stdout= subprocess.PIPE)
	server_files = subprocess.check_output(["ftp", "-p", "-i", "ftp.platform.mediamind.com"], stdin = p1.stdout).split()
	local_files = subprocess.check_output(["ls", sync_dir]).split()
#	print server_files
#	print local_files
	cmd = ""
	for sf in server_files:
		if sf not in local_files and "zip" in sf:
			cmd += "get %s\n"%sf
	cmd = "'" + cmd + "'"
#	print cmd
	p1 = subprocess.Popen(['echo', cmd], stdout=subprocess.PIPE)
	p2 = subprocess.call(["ftp", "-p", "-i", "ftp.platform.mediamind.com"], stdin = p1.stdout)
	return
		
	
def get_Advertiser_dict(cur):
        cur.execute("SELECT AdvertiserName, AdvertiserID FROM SF_Match.Advertisers")
        advert = cur.fetchall()
	adv_dict = {}
	for a in advert:
		tblName = "Std_"+a[0].replace(" ","_")
		adv_dict[a[1]] = tblName
	return adv_dict
def create_Ad_Tables(cur):
	cur.execute("SELECT AdvertiserName, AdvertiserID FROM SF_Match.Advertisers")
	advert = cur.fetchall()
	for a in advert:
#		print a[0], a[1]
		tableName0="Std_" + a[0].replace(" ", "_")
		tableName="DWA_SF_Cookie." +  tableName0
		# check if table exists:
		cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%s'"%tableName0);
		res = cur.fetchall()
		if len(res) == 0:
#			print "Creating table %s"%tableName
			stmt1 = "CREATE TABLE IF NOT EXISTS %s"%tableName + " AS SELECT UserID, EventID, EventTypeID, STR_TO_DATE(EventDate, '%m/%d/%Y %h:%i:%s %p') AS EventDate, CampaignID, SiteID, PlacementID, IP, AdvertiserID,loadTime FROM DWA_SF_Cookie.Std_Akamai WHERE 1=0"
			stmt2 = "ALTER TABLE %s ADD PRIMARY KEY(EventID)"%tableName
			stmt3 = "ALTER TABLE %s ADD INDEX (UserID)"%tableName
			stmt4 = "ALTER TABLE %s ADD INDEX (EventDate)"%tableName
	
			cur.execute(stmt1)
			cur.execute(stmt2)
			cur.execute(stmt3)
			cur.execute(stmt4)
		
	return



def csv_Standard(file_name, cur, tbl_dict={},key_pos=3):
	cur.execute("USE DWA_SF_Cookie")
	# distributes csv file into various Std_CLIENT tables. requires dict and position in each csv line of ID that matches dict key.
	keys_set = False
	x = 0
	with open(file_name,'r') as f:
		while True:
			vals = f.readline()
			stmt_dict = {}
			if vals is None or vals == "": break
			vals = [i.replace("\r\n","") for i in vals.split(",")]	
			if not keys_set:
				col_names = vals
				keys_set = True
			else:
				row_d = {}
				for i in range(len(vals)):
					row_d[col_names[i]] = "'%s'"%vals[i]
				row_d['EventDate'] = "STR_TO_DATE(%s,'%%c/%%e/%%Y %%l:%%i:%%s %%p')"%row_d['EventDate']
				if row_d['AdvertiserID'] not in stmt_dict.keys():
					stmt = "INSERT IGNORE INTO %s (loadTime, UserID, EventID, EventTypeID, EventDate, CampaignID, SiteID, PlacementID, IP, AdvertiserID) VALUES "%tbl_dict[row_d['AdvertiserID'].replace("'","")]
				else:
					stmt = stmt_dict[row_d["AdvertiserID"]] + ","
				stmt += "(NOW(), "+ "%s,"*9
				stmt = stmt%(row_d['UserID'],row_d['EventID'], row_d['EventTypeID'], row_d['EventDate'], row_d['CampaignID'],row_d['SiteID'], row_d['PlacementID'], row_d['IP'], row_d['AdvertiserID'])
				stmt = stmt[:-1]+")"
				stmt_dict[row_d['AdvertiserID']] = stmt
		for k, stmt in stmt_dict.iteritems():
			try:
				cur.execute(stmt)	
			except:
				print "fail: %s"%stmt
				return
	return
def remove_dups(tbl,dup_col, unique_col,cur):
	# Dangerous. Removes duplicates of dup_col.
	cur.execute("select %s FROM %s GROUP BY %s HAVING COUNT(*) > 1"%(dup_col,tbl,dup_col))
	dup_vals = [d[0] for d in cur.fetchall()]
	stmt = "CREATE TABLE tmp_dup_remove SELECT %s, %s FROM %s WHERE %s IN ("%(dup_col, unique_col, tbl, dup_col)
	for d in dup_vals:
		stmt += "'%s',"%d
	stmt = stmt[:-1]+")"
	cur.execute(stmt)
	

	cur.execute("SELECT COUNT(*) FROM tmp_dup_remove")
	before = cur.fetchall()[0]

	stmt = "DELETE FROM tmp_dup_remove a WHERE (a.%s,a.%s) IN (SELECT min(b.%s), b.%s FROM tmp_dup_remove b GROUP BY b.%s)"%(unique_col,dup_col, unique_col,dup_col, dup_col)
	print stmt
	cur.execute(stmt)
	cur.execute("SELECT COUNT(*) FROM tmp_dup_remove")	
	after = cur.fetchall()[0]

	print "found %s rows before, removed %s uniques, have %s left"%(before,len(dup_vals),after)
	
def update_all():
	(file_ls,ftp_cmds) = get_ftp_commands()
	get_ftp_data(ftp_cmds)
def main():
	con,cur = mysql_login.mysql_login()
	adv_dict = get_Advertiser_dict(cur)
	con.autocommit(True)
#	remove_dups("Std_Akamai", "eventID", "loadTime", cur)
#	create_Ad_Tables(cur)	
	files_dir = "/usr/local/var/ftp_sync/downloaded/Standard/"
	files = subprocess.check_output(["ls", files_dir]).split()
	print files
	for f in files:
		csv_Standard(files_dir + f, cur, adv_dict)
	#ftp_sync("/usr/local/var/ftp_sync/downloaded")
	if con:
		con.commit()
		con.close()		
main()
