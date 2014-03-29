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



def initialize(cur,con):
	cur.execute("CREATE DATABASE IF NOT EXISTS attribution")
	cur.execute("USE attribution")
	models = ["last_imp"]

	for m in models:
		stmt = "CREATE TABLE IF NOT EXISTS %s ("%m
		for i in range(1,11):
			stmt += "eventID%s CHAR(36), value%s FLOAT,"%(i,i)
		stmt = stmt[:-1] + ")"
	cur.execute(stmt)
def last_imp(cur,con):
	cur.execute("SELECT conversionID, conversionDate, AdvertiserID FROM DWA_SF_Cookie.MM_Conversion WHERE conversionID NOT IN (SELECT conversionID FROM last_imp)")
	convs = cur.fetchall()
	for conv in convs:
		cur.execute("SELECT ")
	print len(convs)
	

	

def main():
        start = datetime.datetime.now()
        con,cur = mysql_login.mysql_login()
        con.autocommit(False)
	initialize(cur,con)
	last_imp(cur,con)




        if con:
                con.commit()
                con.close()
main()
