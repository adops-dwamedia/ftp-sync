
#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import subprocess
import sys
import os
import re
import datetime
import mysql_login
import cleanup
from warnings import filterwarnings
filterwarnings('ignore', category = mdb.Warning)

def initialize(cur,con):
	cur.execute("CREATE DATABASE IF NOT EXISTS DWA_SF_Cookie")
	cur.execute("USE DWA_SF_Cookie")
	
	# SF_Match needs to be initialized as well
	# configure FTP??
	
	cur.execute("CREATE TABLE IF NOT EXISTS DWA_SF_Cookie.exclude_list "+\
	"(filename VARCHAR(255), ts TIMESTAMP NOT NULL DEFAULT NOW(), PRIMARY KEY(filename))")
	
	
	conv_stmt = "CREATE TABLE IF NOT EXISTS `MM_Conversion` (" +\
	"`userID` char(36) NOT NULL DEFAULT ''," +\
	"`ConversionID` char(36) NOT NULL DEFAULT ''," +\
	"`ConversionDate` datetime NOT NULL," +\
	"`ConversionTagID` varchar(9) DEFAULT NULL," +\
	"`AdvertiserID` int(11) DEFAULT NULL," +\
	"`Revenue` decimal(19,4) DEFAULT NULL," +\
	"`Quantity` int(11) DEFAULT NULL," +\
	"`OrderID` varchar(5) DEFAULT NULL," +\
	"`referrer` varchar(255) DEFAULT NULL," +\
	"`IP` varchar(15) DEFAULT NULL," +\
	"`PlacementID` varchar(255) DEFAULT NULL," +\
	"`SiteID` varchar(255) DEFAULT NULL," +\
	"`CampaignID` varchar(255) DEFAULT NULL," +\
	"`AdGroupID` varchar(255) DEFAULT NULL," +\
	"`String1` varchar(255) DEFAULT NULL," +\
	"`String2` varchar(255) DEFAULT NULL," +\
	"`String3` varchar(255) DEFAULT NULL," +\
	"`String4` varchar(255) DEFAULT NULL," +\
	"`String5` varchar(255) DEFAULT NULL," +\
	"`WinnerEntityID` varchar(255) DEFAULT NULL," +\
	"`EventTypeID` tinyint(4) DEFAULT NULL," +\
	"PRIMARY KEY (`ConversionID`)," +\
	"KEY `ConversionDate` (`ConversionDate`))"

	cur.execute(conv_stmt)
	
	rich_stmt = "CREATE TABLE IF NOT EXISTS`MM_Rich` (" +\
	"`EventID` char(36) NOT NULL DEFAULT ''," +\
	"`UserID` char(36) NOT NULL DEFAULT ''," +\
	"`EventTypeID` tinyint(4) DEFAULT NULL," +\
	"`InteractionID` char(36) DEFAULT NULL," +\
	"`InteractionDuration` varchar(4) DEFAULT NULL," +\
	"`VideoAssetID` varchar(55) DEFAULT NULL," +\
	"`InteractionDate` varchar(255) DEFAULT NULL," +\
	"`EntityID` int(11) DEFAULT NULL," +\
	"`PlacementID` varchar(11) DEFAULT NULL," +\
	"`SiteID` varchar(11) DEFAULT NULL," +\
	"`CampaignID` varchar(11) DEFAULT NULL," +\
	"`BrandID` varchar(11) DEFAULT NULL," +\
	"`AdvertiserID` int(11) DEFAULT NULL," +\
	"`AccountID` int(11) DEFAULT NULL," +\
	"`PCP` varchar(55) DEFAULT NULL," +\
	"PRIMARY KEY (`EventID`,`UserID`, `InteractionDate`)" +\
	") ENGINE=InnoDB DEFAULT CHARSET=latin1"
	
#	cur.execute(rich_stmt)
	
	
	ad_dict = get_ad_dict(cur)
	for k,tblName in ad_dict.iteritems():
		tbl_stmt = "CREATE TABLE IF NOT EXISTS %s ("%tblName +\
		"`UserID` char(36) NOT NULL," +\
		"`EventID` char(36) NOT NULL," +\
		"`EventTypeID` tinyint(4) NOT NULL," +\
		"`EventDate` datetime NOT NULL," +\
		"`CampaignID` mediumint(9) NOT NULL," +\
		"`SiteID` int(11) NOT NULL DEFAULT '0'," +\
		"`EntityID` int(11) NOT NULL DEFAULT '0'," +\
		"`PlacementID` int(11) NOT NULL DEFAULT '0'," +\
		"`IP` varchar(16) NOT NULL DEFAULT ''," +\
		"`AdvertiserID` mediumint(9) NOT NULL DEFAULT '0'," +\
		"`Referrer` varchar(255) NOT NULL DEFAULT ''," +\
		"PRIMARY KEY (`EventID`,`EventDate`)," +\
		"KEY `userID` (`UserID`,`EventDate`)," +\
		"KEY `eventDate` (`EventDate`)" +\
		") ENGINE=InnoDB DEFAULT CHARSET=utf8"
		cur.execute(tbl_stmt)
		partition_by_day(tblName,cur, startDate = -120, endDate = 30)
		
		
	# create union all view
	cur.execute("SHOW TABLES LIKE '%%Std%%'")
	results = [t[0] for t in cur.fetchall()]
	stmt = "CREATE OR REPLACE VIEW MM_Standard AS "
	for tbl in [res for res in results]:
		stmt += "SELECT * FROM %s UNION ALL "%tbl
	stmt = stmt[:-10]
	cur.execute(stmt)
	
	
	
	con.commit()

def get_ad_dict(cur):
	cur.execute("SELECT AdvertiserName, AdvertiserID FROM SF_Match.Advertisers")
	ad_dict = {}
	for n, adid in cur.fetchall():
		ad_dict[adid] = "Std_"+n.replace(" ","_")
	return ad_dict

def match(match_path, cur, con,update_exclude = True):
	cur.execute("SELECT filename FROM DWA_SF_Cookie.exclude_list")
	excludes = [f[0] for f in cur.fetchall()]
	cur.execute("USE SF_Match")
	for f in os.listdir(match_path):
		if re.search("^MM", f) and "CityMatchFile" not in f and f not in excludes:
			print "\tupdating %s"%f

			tableName = re.sub("MM_CLD_Match_", "", f)
			# Mediamind capitalizes its F's unpredictably
			tableName = re.sub("Match[fF]ile.*", "", tableName) 
		
			# open file, read file, decode file, split by newline.
			data = re.sub('"', "",open(match_path+f).read().decode("utf-8-sig")).splitlines()
			if data:
				data = [d.replace(u"\u2122","") for d in data]
				head = data[0].split(",")
				#d_stmt = "DROP TABLE IF EXISTS %s"%tableName 
				stmt = "CREATE TABLE IF NOT EXISTS SF_Match.%s ("%tableName
				# for each column, add an INT column if it is an ID, VARCHAR otherwise.		
				for col in head:
					col = re.sub('"', "", col) # strip quotes
					# detect ID's, use as primary keys.
					if re.match("ID", col):
						
						stmt += "%s INT ,"%col
					else:
						stmt += "%s VARCHAR(255),"%col
				# get rid of last comma, add ending parens
				stmt = stmt[:-1]+ ")"


				#cur.execute(d_stmt)
				cur.execute(stmt)
			
			
				# with table created, insert data. With ID as primary, 
				# INSERT IGNORE ensures no duplication.
				inStmt = "INSERT IGNORE INTO SF_Match.%s VALUES ("%tableName
				inStmt += "%s,"*len(head)
				inStmt = inStmt[:-1] + ")"
				
				batchData = []
				for line in data[1:]:
					row = line.split(",")
					#print row
					batchData.append(tuple(row))
				try:
					cur.executemany(inStmt, batchData)	
					con.commit()
				except:
					# ugh this is lazy but occassionaly match data is malformed
					print "\terror processing matchfile %s"%f
		if update_exclude:
			last_slash = f.rfind("/")
			if last_slash != -1:
				f = f[last_slash+1:]
			cur.execute("INSERT IGNORE INTO DWA_SF_Cookie.exclude_list (filename) VALUES ('%s')"%f)
	con.commit()


def ftp_sync(sync_dir,cur):
	print "syncing ftp server with %s..."%sync_dir
	cur.execute("SELECT filename FROM DWA_SF_Cookie.exclude_list")
	excludes = [f[0] for f in cur.fetchall()]
	p1 = subprocess.Popen(['echo', "nlist" ], stdout= subprocess.PIPE)
	server_files = subprocess.check_output(["ftp", "-p", "-i", "ftp.platform.mediamind.com"], stdin = p1.stdout).split()
	server_files = [sf for sf in server_files if sf[-4:] != "done" and sf != "info"]
	
	server_files = [f for f in server_files if f not in excludes and "test" not in f]
	for f in server_files:
		print "\tfetching %s"%f
		subprocess.call(["wget", "-nc","--reject=done","-q","-P%s"%sync_dir,"ftp://ftp.platform.mediamind.com/%s"%f])
	print "done."
	return
		
	
def partition_by_day(tblName,cur, col="EventDate",startDate = -90, endDate = 30):
	# takes input tbl, and if it is not partitioned, partitions. Else, combine partitions for dates more 
	# than startDate days old 
	cur.execute("USE DWA_SF_Cookie")
	# ugly one liner to get list of dates
	days = [str(datetime.date.today() + datetime.timedelta(days=x)) for x in range(-90,30)]

	# get current partitions
	cur.execute("EXPLAIN PARTITIONS SELECT * FROM %s"%tblName)
	parts = cur.fetchall()[0][3]
	# if table is not partitioned yet
	if parts is None:
		stmt = "ALTER TABLE %s PARTITION BY RANGE Columns (%s) ("%(tblName,col)
		for d in days:
			stmt += "PARTITION `p%s` VALUES LESS THAN ('%s'), "%(d,d)
		stmt += "PARTITION pMAX VALUES LESS THAN (MAXVALUE))"
		cur.execute(stmt)
	else:
		# update partitions. First, find old ones to consolidate.
		old_partitions = parts.split(",")
		new_partitions = ["p" + d for d in days] + ["pMAX"]
		to_consolidate = [op for op in old_partitions if op not in new_partitions] + [new_partitions[0]]

		if len(to_consolidate) > 1:
			stmt = "ALTER TABLE %s REORGANIZE PARTITION "%tblName
			for tc in to_consolidate:
				stmt += "`%s`,"%tc
			stmt = stmt[:-1] + " INTO ( PARTITION `%s` VALUES LESS THAN ('%s') )"%(to_consolidate[-1], to_consolidate[-1].replace("p",""))
#			cur.execute(stmt)

		# add partitions
		to_add = [np for np in new_partitions if np not in old_partitions]
		if len(to_add) > 0:
			stmt = "ALTER TABLE %s REORGANIZE PARTITION pMAX INTO ("%tblName
			for ta in to_add:
				stmt += "PARTITION `%s` VALUES LESS THAN ('%s'),"%(ta, ta.replace("p",""))
			stmt += "PARTITION pMAX VALUES LESS THAN (MAXVALUE))"
			cur.execute(stmt)
		return

		 
		
def csv_Import(file_name,cur,con, update_exclude=True):
	if "Standard" in file_name:
		return
	col_names_set = False

	for l in open(file_name,"r"):

		vals = [i.replace("\r\n","") for i in l.split(",")]	

		if not col_names_set:
			col_names = vals
			col_names_set = True
		else:
			row_d = {}
			for i in range(len(vals)):
				row_d[col_names[i]] = "'%s'"%vals[i]
			# urls may contain ' chars. 
			if "Referrer" in row_d.keys():
				row_d["Referrer"] = "'%s'"%row_d["Referrer"].replace("'","")
				for i in range(len(row_d["Referrer"])):
					if row_d["Referrer"][i] in ["?",";"]:
						row_d["Referrer"] = row_d["Referrer"][:i] + "'"
						break					
				
				
			if "Conversion" in file_name:
				row_d['ConversionDate'] = "STR_TO_DATE(%s,'%%c/%%e/%%Y %%l:%%i:%%s %%p')"%row_d['ConversionDate']
				stmt = "INSERT IGNORE INTO MM_Conversion" + \
				"(UserID, " +\
				"ConversionID, " +\
				"ConversionDate, " +\
				"ConversionTagID, " +\
				"AdvertiserID, " +\
				"Revenue, " +\
				"Quantity, " +\
				"OrderID, " +\
				"referrer, " +\
				"IP, " +\
				"WinnerEntityID, " +\
				"PlacementID, " +\
				"SiteID, " +\
				"CampaignID, " +\
				"AdGroupID, " +\
				"EventTypeID, " +\
				"String1, " +\
				"String2, " +\
				"String3, " +\
				"String4, " +\
				"String5) VALUES ("
				stmt_add = "%s,"*20 + "%s)"
				stmt_add = stmt_add%(
				row_d['UserID'], 
				row_d['ConversionID'], 
				row_d['ConversionDate'], 
				row_d['ConversionTagID'], 
				row_d['AdvertiserID'], 
				row_d['Revenue'], 
				row_d['Quantity'], 
				row_d['OrderID'], 
				row_d['Referrer'], 
				row_d['IP'],
				row_d['WinnerEntityID'],
				row_d['PlacementID'],
				row_d['SiteID'],
				row_d['CampaignID'],
				row_d['AdGroupID'],
				row_d['EventTypeID'],
				row_d['String1'],
				row_d['String2'],
				row_d['String3'],
				row_d['String4'],
				row_d['String5']
				)

								
			if "Rich" in file_name:
				row_d['InteractionDate'] = "STR_TO_DATE(%s,'%%c/%%e/%%Y %%l:%%i:%%s %%p')"%row_d['InteractionDate']
				stmt = "INSERT IGNORE INTO MM_Rich" + \
				"(EventID, " +\
				"UserID, " +\
				"EventTypeID, " +\
				"InteractionID, " +\
				"InteractionDuration, " +\
				"VideoAssetID, " +\
				"InteractionDate, " +\
				"EntityID, " +\
				"PlacementID, " +\
				"SiteID, " +\
				"CampaignID, " +\
				"BrandID," +\
				"AdvertiserID, " +\
				"AccountID, " +\
				"PCP) " +\
				"VALUES ("
				stmt_add = "%s,"*14 + "%s)"
				stmt_add = stmt_add%(
				row_d['EventID'],
				row_d['UserID'], 
				row_d['EventTypeID'],
				row_d['InteractionID'],
				row_d['InteractionDuration'],
				row_d['VideoAssetID'],
				row_d['InteractionDate'],
				row_d['EntityID'],
				row_d['PlacementID'],
				row_d['SiteID'],
				row_d['CampaignID'],
				row_d['BrandID'],
				row_d['AdvertiserID'],
				row_d['AccountID'],
				row_d['PCP'])
			stmt = stmt + stmt_add
			cur.execute(stmt)				
		
	if update_exclude:
		last_slash = file_name.rfind("/")
		if last_slash != -1:
			file_name = file_name[last_slash+1:] 
		cur.execute("INSERT IGNORE INTO DWA_SF_Cookie.exclude_list (filename) VALUES ('%s')"%file_name)
	
	
def csv_Standard(file_name, ad_dict, cur,con, insert_interval = 1, print_interval = 1, update_exclude=True):
	cur.execute("USE DWA_SF_Cookie")
	# distribute Standard CSV file into DB tables for each Advertiser.

	# initialize. col_names_set is variable to tell if we have loaded column names yet.
	# line_i and start are vars for benchmarking.
	 
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
				row_d[col_names[i]] = "'%s'"%vals[i].replace("'","")
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
			if adID not in insert_d.keys():
				adID = 0
			stmt = insert_d[adID]["stmt"]
			
					
	
			# if statement is empty, initialize. Else just add to it.
			if stmt == "": 
				stmt = "INSERT IGNORE INTO %s "%insert_d[adID]["tblName"] + \
				"(UserID, " + \
				"EventID, " + \
				"EventTypeID, " + \
				"EventDate, " + \
				"EntityID, " +\
				"CampaignID, " + \
				"SiteID," + \
				"PlacementID, " + \
				"IP, "+ \
				"AdvertiserID,"  + \
				"Referrer) VALUES "
				
			stmt_add = "(" + "%s,"*10 + "%s),"
			stmt_add = stmt_add%(row_d['UserID'],
			row_d['EventID'],
			row_d['EventTypeID'],
			row_d['EventDate'],
			row_d['EntityID'],
			row_d['CampaignID'],
			row_d['SiteID'],
			row_d['PlacementID'],
			row_d['IP'],
			row_d['AdvertiserID'],
			row_d["Referrer"])
			stmt += stmt_add	
	
			# increment records, check if execution is needed.
			records = insert_d[adID]["records"] + 1
			if records == insert_interval:
				stmt = stmt[:-1] # remove trailing comma

				# benchmarking:
				time_elapsed = (datetime.datetime.now()-overall_start).seconds + 1 # avoid div by zero
				records_per_second = line_i/time_elapsed
				print "\t%s records from %s, %s records into %s: time: %s, %s records per sec"%(
				line_i, file_name[-13:-4], records,insert_d[adID]["tblName"],time_elapsed, records_per_second)
				
				# reset
				try:
					cur.execute(stmt)
				except:
					# to deal with occasional problems, simply skip indivudal records. 
					# first, take print_interval to 1. If it is already 1, skip.
					print "Exception executing SQL"
					if print_interval > 1:
						print "restarting, one record at a time.\n\n\n\n"
						return load_all_Standard(Std_dir,cur,con, 1)
					else:
						print "Skipping record."
				
				stmt, records = "",0
				
			insert_d[adID]["stmt"], insert_d[adID]["records"] = stmt, records
			line_i += 1
				
	# done looping, execute all remaining statements.
	for adID, vals in insert_d.iteritems():
		stmt = vals["stmt"]
		if stmt != "":
			stmt = stmt[:-1]
			try:
				cur.execute(stmt)	
			except Exception as e:
				print "stmt failed: %s"%stmt
				raise e
				
	if update_exclude:
		last_slash = file_name.rfind("/")
		if last_slash != -1:
			file_name = file_name[last_slash+1:] 
		cur.execute("INSERT IGNORE INTO DWA_SF_Cookie.exclude_list (filename) VALUES ('%s')"%file_name)
			
	return
	
	
  
def unzip_all(zip_dir, cur, add_to_exclude=True):
	cur.execute("SELECT filename FROM DWA_SF_Cookie.exclude_list")
	excludes = [x[0] for x in cur.fetchall()]
	files = subprocess.check_output(["ls", "%s/"%zip_dir]).split()
	files = [f for f in files if f[-3:] == "zip"]
	for fileType in ["Rich", "Standard", "Conversion", "Match"]:
		unzip_dir = zip_dir + "/%s/"%fileType
		for f in files:
			if f not in excludes and fileType in f:
				unzipProc = subprocess.Popen(["unzip", "-u", zip_dir+f, "-d", unzip_dir],
				stderr=subprocess.PIPE, stdout = subprocess.PIPE)
				unzipOutput, unzipErr = unzipProc.communicate()
				ret = unzipProc.returncode
				
				if ret == 0:
					cur.execute("INSERT IGNORE INTO DWA_SF_Cookie.exclude_list (filename) VALUES ('%s')"%f)
				if ret == 9:
					print "%s does not appear to be a valid zipfile. Deleting file and entry in DWA_SF_Cookie.exclude_list"%f
					subprocess.call(["rm", zip_dir+f])
					cur.execute("DELETE FROM DWA_SF_Cookie.exclude_list WHERE filename = '%s'"%f)

def load_all_Standard(files_dir,cur,con,insert_interval = 1000):
	files = subprocess.check_output(["ls", files_dir]).split()	
	cur.execute("SELECT filename FROM DWA_SF_Cookie.exclude_list")	
	excludes = [x[0] for x in cur.fetchall()]	

	ad_dict = get_ad_dict(cur)
	
	files = [f for f in files if "Standard" in f and f not in excludes]	
	for f in files:

		ret = csv_Standard(files_dir + f, ad_dict,cur,con,insert_interval)
		if ret is None:
			cur.execute("INSERT IGNORE INTO DWA_SF_Cookie.exclude_list (filename) VALUES ('%s')"%f)
			con.commit()			
def load_all(files_dir, cur,con):
	# allows for array of directories to be passed.	
	if type(files_dir) is not list:
		files_dir = [files_dir]

	cur.execute("SELECT fileName from DWA_SF_Cookie.exclude_list")
	excludes = [f[0] for f in cur.fetchall()]

	for fd in files_dir:
		files = subprocess.check_output(["ls", fd]).split()	
		files = [f for f in files if f[-3:] == "csv" and f not in excludes]
		for f in files:
			print "\tinserting %s"%f
			csv_Import(fd + f,cur,con, update_exclude=True)
			con.commit()
			
				
				
if __name__ == "__main__":
	start = datetime.datetime.now()
	con,cur = mysql_login.mysql_login()
	con.autocommit(False)
	ftp_sync("/usr/local/var/ftp_sync/downloaded",cur)
	unzip_all("/usr/local/var/ftp_sync/downloaded/", cur)
	try:
		match("/usr/local/var/ftp_sync/downloaded/Match/",cur,con)
	except:
		print "Match file method raised error"
	initialize(cur,con)
	#partition_by_day("MM_Rich",cur, col="InteractionDate",startDate = -120, endDate = 30)

#	Rich and Conversion files
	load_all(["/usr/local/var/ftp_sync/downloaded/Conversion/","/usr/local/var/ftp_sync/downloaded/Rich/"],cur,con)
	
	Std_dir = "/usr/local/var/ftp_sync/downloaded/Standard/"
	load_all_Standard(Std_dir,cur,con, 1000)
	end = datetime.datetime.now()
	print "db updated in %s seconds"%(end-start).seconds
		
	print "cleaning up old files and partitions..."

        cleanup.partitionDrop(cur)
        cleanup.drop_raw_files()
	print "done."
	if con:
		con.commit()
		con.close()	
		
		
