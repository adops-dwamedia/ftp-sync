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

user="tomb"
host="localhost"
db="DWA_SF_Cookie"
pw='DW4mediatb'


# connect to db
try:
	con = mdb.connect(host, user, pw, db)
	cur = con.cursor()
	con.set_character_set('utf8')
	con.autocommit(True)
	cur.execute('SET NAMES utf8;') 
	cur.execute('SET CHARACTER SET utf8;')
	cur.execute('SET character_set_connection=utf8;')

        
except mdb.Error, e:

	print "Error %d: %s" % (e.args[0], e.args[1])
	sys.exit(1)



def get_ftp_commands():

# checks import_log table and returns two arrays: one with ftp commands and one with the
# list of files to be downloaded.
	#retrieve list of files available
	print "retrieving list of files"
	file_ls_raw = subprocess.check_output("""ftp -pid ftp.platform.mediamind.com  << EOF
	nlist 
	quit
	EOF""", shell=True).split("\n")
	file_ls = []
	for i in range(len(file_ls_raw)):
		if file_ls_raw[i][-3:] == "zip":
			file_ls.append(file_ls_raw[i])

	#retrieve list of files in db, remove files already imported
	cur.execute("SELECT DISTINCT fileName FROM import_log")
	for f in cur.fetchall():
		if f[0] in file_ls: file_ls.remove(f[0])
	

	ftp_cmds = ""
	for f in file_ls:
		ftp_cmds += "get %s %s/%s\n"%(f, data_path, f)
#		print ftp_cmds 	
	return file_ls, ftp_cmds
 	
	 
#(file_ls,ftp_cmds) = get_ftp_commands()
 
 
 
def get_ftp_data(ftp_cmds):
#	print ftp_cmds
	download_cmd_file = open ("%s/downloadCmds"%tmp_path, "w")
	download_cmd_file.write(ftp_cmds)
	os.system("echo 'starting ftp import process at '`date`")
	#ftp_import = "ftp -vpi ftp.platform.mediamind.com < %s/downloadCmds 2>> %s/ftp.err 1>> %s/ftp.log"%(tmp_path, log_path, log_path)
	
	ftp_import = "ftp -vpi ftp.platform.mediamind.com < %s/downloadCmds "%tmp_path
#	print ftp_import
	subprocess.Popen(['ftp', '-pvi', 'ftp.platform.mediamind.com'], stdin=download_cmd_file)	
	return
#get_ftp_data(ftp_cmds)

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



create_Ad_Tables()




def insert_csv(file_name, target_table, standard_table = False):
	mv_cmd = "mv %s %s"%(file_name, tmp_path+"/"+target_table)
	os.system(mv_cmd)
	print mv_cmd
	os.system("mysqlimport -u%s -p%s --local --ignore-lines=1 --ignore --fields-terminated-by=\",\" --lines-terminated-by=\"\n\" %s %s"%(user, pw, db, tmp_path+ "/" +target_table))
	if not standard_table:
		return 	
        cur.execute("SELECT AdvertiserName, AdvertiserID FROM SF_Match.Advertisers")
        advert = cur.fetchall()
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
#insert_csv("/usr/local/var/ftp_sync/downloaded/Standard/test.csv", "MM_Standard_tmp", True)


def main():
	zip_dir = "/usr/local/var/ftp_sync/downloaded/"
	ls_files = 	subprocess.check_output(['ls',zip_dir]).split()
	for l in ls_files:
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
main()
'''
"ftp -vpi ftp.platform.mediamind.com < $TMP_PATH/downloadCmds 2>> $LOG_PATH/ftpErrors.log >> $LOG_PATH/ftpLog
# def insert_csv()
# def update_advertiser_tables()
# def update_match()














os.system("bash /usr/local/ftp_sync/bin/ftpImport.sh")

os.system("python /usr/local/ftp_sync/bin/match.py")

host = 'localhost'
user = 'tomb'
pw = 'DW4mediatb'
db = 'DWA_SF_Cookie'

log_path="/usr/local/ftp_sync/logs"

# get db handle:
try:
	con = mdb.connect(host, user, pw, db)
	cur = con.cursor()
	con.set_character_set('utf8')
	con.autocommit(True)
	cur.execute('SET NAMES utf8;') 
	cur.execute('SET CHARACTER SET utf8;')
	cur.execute('SET character_set_connection=utf8;')

        
except mdb.Error, e:

	print "Error %d: %s" % (e.args[0], e.args[1])
	sys.exit(1)

cur.execute("SELECT AdvertiserName, AdvertiserID FROM SF_Match.Advertisers")
advert = cur.fetchall()
for a in advert:
	print a[0], a[1]
	tableName0=a[0].replace(" ", "_")+"_Std"
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

	print "updating %s"%tableName	
	stmt5 = "INSERT INTO %s"%tableName + " (SELECT UserID, EventID, EventTypeID, STR_TO_DATE(EventDate, '%m/%d/%Y %h:%i:%s %p'), CampaignID, SiteID, PlacementID, IP, AdvertiserID FROM DWA_SF_Cookie.MM_Standard_tmp WHERE AdvertiserID ="+" %s"%a[1] + ") ON DUPLICATE KEY UPDATE EventID = Values(EventID)"
	print stmt5
	os.system("echo %s update begun at %s>> %s/adTables.log"%(tableName, datetime.datetime.now(), log_path))
	cur.execute(stmt5)
	os.system("echo %s update completed at %s>> %s/adTables.log"%(tableName, datetime.datetime.now(), log_path))
	
cur.execute("INSERT INTO MM_Standard SELECT * FROM MM_Standard_tmp")



cur.execute("TRUNCATE  MM_Standard_tmp")	





con.commit()

if con:
	con.close()
'''
