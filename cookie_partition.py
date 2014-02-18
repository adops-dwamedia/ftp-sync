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

	con.set_character_set('utf8')
	cur.execute('SET NAMES utf8;') 
	cur.execute('SET CHARACTER SET utf8;')
	cur.execute('SET character_set_connection=utf8;')

 
if con:
	con.close()





def create_partitions(table, col, cur):
# creates partition by some id, with one partition per id. Also creates null lower
# partition and maxvalue high one. The high partition can later be divided into distinct
# id partitions. Partitions by id + 1 so each id value can have own partition.
	cur.execute("SELECT DISTINCT %s + 1 FROM %s ORDER BY %s"%(col, table, col)
	distincts = cur.fetchall()
	stmt = "ALTER TABLE %s PARTITION BY RANGE(%s) ("%(table,col)
	print distincts
	for d in distincts:
		stmt += "PARTITION p%s VALUES"
create_partitions("test.test", "id", cur)

	
	