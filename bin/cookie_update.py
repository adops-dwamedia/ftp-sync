#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import subprocess
import sys
import os
import re
import datetime
from warnings import filterwarnings
filterwarnings('ignore', category = mdb.Warning)

home_path="/usr/local/ftp_sync/bin"
sql_path="/usr/local/ftp_sync/SQL/"
data_path="/usr/local/var/ftp_sync/downloaded/"
log_path="/usr/local/ftp_sync/logs/"
tmp_path="/usr/local/ftp_sync/var/"


def ftp_sync(sync_dir):
	p1 = subprocess.Popen(['echo', "nlist . %s"%sync_dir], stdout= subprocess.PIPE)
	server_files = subprocess.check_output(["ftp", "-p", "-i", "ftp.platform.mediamind.com"], stdin = p1.PIPE).split()
	local_files = subprocess.check_output(["ls", sync_dir]).split()

	cmd = ""
	for sf in server_files:
		if sf not in local_files and "zip" in sf:
			cmd += "get %s;"
	cmd = "'" + cmd + "'"

	p1 = subprocess.Popen(['echo', cmd], stdout=subprocess.PIPE)
	p2 = subprocess.Popen(["ftp", "-p", "-i", "ftp.platform.mediamind.com"], stdin = p1.PIPE)
	return
		
	


def create_Ad_Tables():
	cur.execute("SELECT AdvertiserName, AdvertiserID FROM SF_Match.Advertisers")
	advert = cur.fetchall()
	for a in advert:
	#	print a[0], a[1]
		tableName0="Std_" + a[0].replace(" ", "_")
		tableName="DWA_SF_Cookie." +  tableName0
		# check if table exists:
		cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%s'"%tableName0);
		res = cur.fetchall()
		if len(res) == 0:
			print "Creating table %s"%tableName
			stmt1 = "CREATE TABLE IF NOT EXISTS %s"%tableName + " AS SELECT UserID, EventID, EventTypeID, STR_TO_DATE(EventDate, '%m/%d/%Y %h:%i:%s %p') AS EventDate, CampaignID, SiteID, PlacementID, IP, AdvertiserID FROM DWA_SF_Cookie.MM_Standard_tmp WHERE 1=0"
			stmt2 = "ALTER TABLE %s ADD PRIMARY KEY(EventID)"%tableName
			stmt3 = "ALTER TABLE %s ADD INDEX (UserID)"%tableName
			stmt4 = "ALTER TABLE %s ADD INDEX (EventDate)"%tableName
	
			cur.execute(stmt1)
			cur.execute(stmt2)
			cur.execute(stmt3)
			cur.execute(stmt4)
	return

def insert_csv(file_name, target_table, standard_table = False, target_account = False):
	mv_cmd = "mv %s %s"%(file_name, tmp_path+"/"+target_table)
	print mv_cmd
	return_code = os.system(mv_cmd)
	if return_code != 0:
		raise Exception("os.system returned error code")
	os.system("mysqlimport -u%s -p%s --local --ignore-lines=1 --ignore --fields-terminated-by=\",\" --lines-terminated-by=\"\n\" %s %s"%(user, pw, db, tmp_path+ "/" +target_table))
	if not standard_table:
		return 	
        cur.execute("SELECT AdvertiserName, AdvertiserID FROM SF_Match.Advertisers")
        advert = cur.fetchall()
	if target_account:
		advert = target_account
	for a in advert:
		tableName0="Std_"+a[0].replace(" ", "_")
		tableName="DWA_SF_Cookie." +  tableName0
	#	print "updating %s"%tableName	
		stmt5 = "INSERT INTO %s"%tableName + " (SELECT UserID, EventID, EventTypeID, STR_TO_DATE(EventDate, '%m/%d/%Y %h:%i:%s %p'), CampaignID, SiteID, PlacementID, IP, AdvertiserID FROM DWA_SF_Cookie.MM_Standard_tmp WHERE AdvertiserID ="+" %s"%a[1] + ") ON DUPLICATE KEY UPDATE EventID = Values(EventID)"
	#	print stmt5
		os.system("echo %s update begun at %s>> %s/adTables.log"%(tableName, datetime.datetime.now(), log_path))
		cur.execute(stmt5)
		os.system("echo %s update completed at %s>> %s/adTables.log"%(tableName, datetime.datetime.now(), log_path))
	
	cur.execute("INSERT INTO MM_Standard SELECT * FROM MM_Standard_tmp")
	cur.execute("TRUNCATE  MM_Standard_tmp")	
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
	insert_all()
main()
