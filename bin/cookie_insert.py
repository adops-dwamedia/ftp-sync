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
	cmd = ""
	for sf in server_files:
		if sf not in local_files and "zip" in sf:
			cmd += "get %s\n"%sf
	cmd = "'" + cmd + "'"
	p1 = subprocess.Popen(['echo', cmd], stdout=subprocess.PIPE)
	p2 = subprocess.call(["ftp", "-p", "-i", "ftp.platform.mediamind.com"], stdin = p1.stdout)
	return
		
	
def csv_Standard(file_name, cur,con, insert_interval = 1000):
	cur.execute("USE DWA_SF_Cookie")
	keys_set = False
	line_i = 0
	stmt = ""
	print "inserting from %s, printing times for insertion of %s records"%(file_name,insert_interval)
	start = datetime.datetime.now()
	for l in open(file_name,"r"):
		if line_i%insert_interval == 0 and stmt != "": 
			print str(datetime.datetime.now() - start)+",%s"%line_i
			start = datetime.datetime.now()
			stmt = stmt[:-1]
			try:
				cur.execute(stmt)
			except:
				if insert_interval == 1:
					print "fail: %s"%stmt
				else:
					print "fail: %s"%file_name
		
				return
			stmt = ""
		line_i += 1
		vals = [i.replace("\r\n","") for i in l.split(",")]	
		if not keys_set:
			col_names = vals
			keys_set = True
		else:
			row_d = {}
			for i in range(len(vals)):
				row_d[col_names[i]] = "'%s'"%vals[i]
			# urls may contain ' chars. 
			row_d["Referrer"] = "'%s'"%row_d["Referrer"].replace("'","")
			row_d['EventDate'] = "STR_TO_DATE(%s,'%%c/%%e/%%Y %%l:%%i:%%s %%p')"%row_d['EventDate']
			# remove query string from referrer
			qs_ls = [row_d["Referrer"].find("?"), row_d["Referrer"].find(";")]
			qs_ls = filter(lambda a: a != -1, qs_ls)
			if len(qs_ls) > 0:
				qs = min(qs_ls)
				before = row_d["Referrer"]
				row_d["Referrer"] = "'%s'"%row_d["Referrer"][:qs].replace("'","")
				after = row_d["Referrer"]

			if stmt == "": 
				stmt = "INSERT IGNORE INTO MM_Standard (UserID, EventID, EventTypeID, EventDate, CampaignID, SiteID, PlacementID, IP, AdvertiserID,Referrer) VALUES "
			stmt_add = "(" + "%s,"*9 + "%s),"
			stmt_add = stmt_add%(row_d['UserID'],row_d['EventID'], row_d['EventTypeID'], row_d['EventDate'], row_d['CampaignID'],row_d['SiteID'], row_d['PlacementID'], row_d['IP'], row_d['AdvertiserID'], row_d["Referrer"])
			stmt += stmt_add
	stmt = stmt[:-1]
	cur.execute(stmt)	
	con.commit()
	return
	
def unzip_all(zip_dir, unzip_dir, fileType, cur, add_to_exclude=True):
        cur.execute("SELECT filename FROM exclude_list")
        excludes = [x[0] for x in cur.fetchall()]
	files = subprocess.check_output(["ls", zip_dir]).split()
	for f in files:
	#	print f
		if f not in excludes and fileType in f:
			ret = subprocess.call(["unzip", "-u", zip_dir+f, "-d", unzip_dir])
			if ret == 0:
				cur.execute("INSERT INTO exclude_list VALUES ('%s')"%f)
	

	
def main():
	con,cur = mysql_login.mysql_login()
	con.autocommit(False)
#	unzip_all("/usr/local/var/ftp_sync/downloaded/", "/usr/local/var/ftp_sync/downloaded/Standard/","Standard", cur)
	#create_Ad_Tables(cur)	
	files_dir = "/usr/local/var/ftp_sync/downloaded/Standard/"
	files = subprocess.check_output(["ls", files_dir]).split()
	
	cur.execute("SELECT filename FROM exclude_list")
	excludes = [x[0] for x in cur.fetchall()]
	for f in files:
		if "Standard" in f:# and f not in excludes:
			ret = csv_Standard(files_dir + f, cur,con)
			if ret is None:
				cur.execute("INSERT INTO exclude_list VALUES ('%s')"%f)
				con.commit()
	#ftp_sync("/usr/local/var/ftp_sync/downloaded")
	if con:
		con.commit()
		con.close()		
main()
