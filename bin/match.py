#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import sys
import os
import re
from warnings import filterwarnings
import mysql_login
filterwarnings('ignore', category = mdb.Warning)



# for each file, extract SQL table name from file name, 
# obtain schema from first line of file, and create/insert into table as needed.
# This assumes that match files will not overlap ID's
def match(match_path, cur):
	for f in os.listdir(match_path):
		if re.search("^MM", f) and "CityMatchFile" not in f:
			tableName = re.sub("MM_CLD_Match_", "", f)
			print "updating %s"%tableName
			# Mediamind capitalizes its F's unpredictably
			tableName = re.sub("Match[fF]ile.*", "", tableName) 
		
			# open file, read file, decode file, split by newline.
			data = re.sub('"', "",open(match_path+f).read().decode("utf-8-sig")).splitlines()
			data = [d.replace(u"\u2122","") for d in data]
			head = data[0].split(",")
			#d_stmt = "DROP TABLE IF EXISTS %s"%tableName 
			stmt = "CREATE TABLE IF NOT EXISTS %s ("%tableName
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
			inStmt = "INSERT IGNORE INTO %s VALUES ("%tableName
			inStmt += "%s,"*len(head)
			inStmt = inStmt[:-1] + ")"
			
			batchData = []
			for line in data[1:]:
				row = line.split(",")
				#print row
				batchData.append(tuple(row))
				
			cur.executemany(inStmt, batchData)
def main():
	con,cur = mysql_login.mysql_login()	
	match("/usr/local/var/ftp_sync/downloaded/Match/unzipped/", cur)
	if con:
		con.close()
main()
