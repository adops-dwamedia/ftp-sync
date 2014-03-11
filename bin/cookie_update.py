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
	p1 = subprocess.Popen(['echo', "nlist . %s"%sync_dir], stdout= subprocess.PIPE)
	server_files = subprocess.check_output(["ftp", "-p", "-i", "ftp.platform.mediamind.com"], stdin = p1.stdout).split()
	local_files = subprocess.check_output(["ls", sync_dir]).split()

	cmd = ""
	for sf in server_files:
		if sf not in local_files and "zip" in sf:
			cmd += "get %s;"
	cmd = "'" + cmd + "'"

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
		print a[0], a[1]
		tableName0="Std_" + a[0].replace(" ", "_")
		tableName="DWA_SF_Cookie." +  tableName0
		# check if table exists:
		cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%s'"%tableName0);
		res = cur.fetchall()
		if len(res) == 0:
			print "Creating table %s"%tableName
			stmt1 = "CREATE TABLE IF NOT EXISTS %s"%tableName + " AS SELECT UserID, EventID, EventTypeID, STR_TO_DATE(EventDate, '%m/%d/%Y %h:%i:%s %p') AS EventDate, CampaignID, SiteID, PlacementID, IP, AdvertiserID FROM DWA_SF_Cookie.Std_Akamai WHERE 1=0"
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
				stmt = "INSERT IGNORE INTO %s (UserID, EventID, EventTypeID, EventDate, CampaignID, SiteID, PlacementID, IP, AdvertiserID) VALUES ("%tbl_dict[row_d['AdvertiserID'].replace("'","")]
				stmt += "%s,"*9
				stmt = stmt%(row_d['UserID'],row_d['EventID'], row_d['EventTypeID'], row_d['EventDate'], row_d['CampaignID'],row_d['SiteID'], row_d['PlacementID'], row_d['IP'], row_d['AdvertiserID'])
				stmt = stmt[:-1]+")"
				print stmt
				cur.execute(stmt)	
	return

def insert_all():
	print "insert all"
	zip_dir = "/usr/local/var/ftp_sync/downloaded/"
	ls_files = 	subprocess.check_output(['ls',zip_dir]).split()
	print ls_files
	for l in ls_files:
		sql_errors = False
		if "Standard" in l and "zip" in l:
			print l
			cmd = "unzip -xu %s -d%s/Standard"%(zip_dir + "/" + l, zip_dir)
			print cmd			
			os.system(cmd)
			for csv_file in subprocess.check_output(['ls', zip_dir+"/Standard"]).split():
				try:
					insert_csv(zip_dir + "/Standard/" + csv_file, "MM_Standard_tmp", True)
				except:
					cmd = "echo 'import of %s failed' >> %s"%(csv_file,log_path) + "/mysqlImport.err"
					os.system(cmd)
					return
					sql_errors = True
			if not sql_errors:
				stmt = "INSERT INTO import_log VALUES ('%s', CURRENT_DATE()) ON DUPLICATE KEY UPDATE importDate = CURRENT_DATE()"%l
				print stmt
				cur.execute(stmt)
			mv_cmd = "mv "+zip_dir + l + " "+zip_dir+"/inSql/"
			print mv_cmd
			os.system(mv_cmd)
		elif "Conversion" in l and "zip" in l:
			cmd = "unzip -xu %s -d%s/Conversion"%(zip_dir + "/" + l, zip_dir)
			print cmd
			os.system(cmd)
			for csv_file in subprocess.check_output(['ls', zip_dir+"/Conversion"]).split():
				try:
					insert_csv(zip_dir + "/Conversion/" + csv_file, "MM_Conversion")
				except:
					cmd = "echo 'import of %s failed' >> %s"%(csv_file,log_path) + "/mysqlImport.err"
					os.system(cmd)
					sql_errors = True
	return

def update_all():
	(file_ls,ftp_cmds) = get_ftp_commands()
	get_ftp_data(ftp_cmds)
def main():
	con,cur = mysql_login.mysql_login()
	adv_dict = get_Advertiser_dict(cur)
	con.autocommit(True)
#	create_Ad_Tables(cur)	
	csv_Standard("/usr/local/var/ftp_sync/downloaded/test.csv", cur,adv_dict)
	if con:
		con.commit()
		con.close()		
main()
