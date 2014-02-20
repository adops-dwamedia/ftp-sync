#!/usr/bin/python 
# -*- coding: utf-8 -*- 
import MySQLdb as mdb 
import sys 
import os
import re
from warnings import filterwarnings

filterwarnings('ignore', category = mdb.Warning) 

host = 'localhost' 
user = 'tomb' 
pw = 'DW4mediatb' 
db = 'DWA_SF_Cookie' 

# get db handle: 
try: 
	con = mdb.connect(host, user, pw, db) 
	cur = con.cursor() 
except mdb.Error, e: 
	print "Error %d: %s" % (e.args[0], e.args[1]) 
	sys.exit(1) 

def create_partitions(table, col, cur):
# creates partition by some id, with one partition per id. Also creates null lower 
# partition and maxvalue high one. The high partition can later be divided into distinct 
# id partitions. Partitions by id + 1 so each id value can have own partition.  
	cur.execute("SELECT DISTINCT %s + 1 FROM %s ORDER BY %s"%(col, table, col)) 
	distincts = cur.fetchall() 
	stmt = "ALTER TABLE %s PARTITION BY RANGE(%s) ("%(table,col) 
	distincts = [d[0] for d in distincts] 
	for d in distincts: 
		stmt += "PARTITION p%s VALUES LESS THAN (%s),"%(d, d) 
	stmt += " PARTITION pMAX VALUES LESS THAN MAXVALUE)" 
	cur.execute(stmt) 
def update_partitions(table,col,cur): 
	cur.execute("SHOW CREATE TABLE %s"%(table)) 
	raw = cur.fetchall() 
	names = re.findall(r'PARTITION (.*) VALUES', raw[0][1]) 
	values = re.findall(r'VALUES LESS THAN \((.*)\)', raw[0][1]) 
	# create list of stmts to check the number of distinct values in each partition.  
	stmts = [] 
	for i in range(len(values)): 
		stmt = "SELECT DISTINCT(%s+1) FROM %s WHERE %s "%(col,table,col) 
		if i == 0: 
			stmt += ">= -100000 AND %s < %s"%(col, values[i]) 
		else: 
			stmt += ">= %s AND %s < %s"%(values[i-1], col, values[i]) 
		stmt += " ORDER BY %s"%col
		stmts.append(stmt) 
	stmts.append("SELECT DISTINCT %s+1 FROM %s WHERE %s >= %s ORDER BY %s"%(col,table,col,values[-1], col)) 
	
	# loop through statements, and create a new partition if any contain multiple values. 
	# last stmt (maxvalue) is a special case and is handled separately

	for i in range(len(stmts)):
		print stmts[i]
		cur.execute(stmts[i])
		ds = cur.fetchall()
		print ds
		if len(ds) > 1 or (len(ds) > 0 and i == len(stmts)-1):
			stmt = "ALTER TABLE %s REORGANIZE PARTITION %s INTO("%(table, names[i])
			for j in range(len(ds)):
				stmt += "PARTITION p%s VALUES LESS THAN (%s),"%(ds[j][0],ds[j][0])
				
				# if we are on the last partition, create empty MAXVALUE partition to collect future new values
				if i == len(stmts)-1 and j == len(ds)-1:
					stmt += "PARTITION pMAX VALUES LESS THAN MAXVALUE,"
			stmt = stmt[:-1] + ")"
			print stmt
			cur.execute(stmt)
update_partitions("test.test", "to_days(d)", cur)


	
if con:
	con.close()

