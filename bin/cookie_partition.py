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
	print "Error %d: %s" % (e.args[0], e.args[1]) sys.exit(1) 

def create_partitions(table, col, cur):
# creates partition by some id, with one partition per id. Also creates null lower 
# partition and maxvalue high one. The high partition can later be divided into distinct 
# id partitions. Partitions by id + 1 so each id value can have own partition.  
	cur.execute("SELECT DISTINCT %s + 1 FROM %s ORDER BY %s"%(col, table, col)) 
	distincts = cur.fetchall() 
	stmt = "ALTER TABLE %s PARTITION BY RANGE(%s) ("%(table,col) 
	distincts = [d[0] for d in distincts] 
	print distincts	
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
		stmt = "SELECT DISTINCT(%s) FROM %s WHERE %s "%(col,table,col) 
		if i == 0: 
			stmt += ">= -100000 AND %s < %s"%(col, values[i]) 
	else: 
		stmt += ">= %s AND %s < %s"%(values[i-1], col, values[i]) 
		stmts.append(stmt) stmts.append("SELECT DISTINCT %s FROM %s WHERE %s >= %s"%(col,table,col,values[-1])) print len(stmts) == len(names) for i in range(len(stmts)):
		cur.execute(stmt)
		ds = cur.fetchall()
		print ds, len(ds)


update_partitions("test.test", "id", cur)


	
if con:
	con.close()

