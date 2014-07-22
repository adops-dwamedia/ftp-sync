#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import mysql_login
import time
import datetime
import subprocess
import re
from warnings import filterwarnings
filterwarnings('ignore', category = mdb.Warning)        





def partitionDrop(cur,db = "DWA_SF_Cookie",d = datetime.date.today() - datetime.timedelta(days = 30*6)):
	if type(d) == datetime.date:
		d = d.timetuple()

	cur.execute("USE %s"%db)

	cur.execute("SHOW TABLES")

	tbls = [t[0] for t in cur.fetchall() if t[0] not in [ "MM_Standard"]]

	for t in tbls:
		cur.execute("EXPLAIN PARTITIONS SELECT * FROM %s"%t)
		parts = cur.fetchall()[0][3]
		if parts is not None:
			parts_d = {p:time.strptime(p[1:],"%Y-%m-%d")  for p in parts.split(",") if p != "pMAX"}

			parts_d = {k:parts_d[k] for k in parts_d if parts_d[k] < d }
			if parts_d != {}:
				print "pruning %s"%t
				print "partitions: ", parts_d

			for k in parts_d.keys():
				stmt = "alter table %s drop partition `%s`"%(t, k)
				cur.execute(stmt)

def drop_raw_files(data_dir = "/usr/local/var/ftp_sync/downloaded", cutoff_date = datetime.date.today() - datetime.timedelta(days = 30*6)):
	if type(cutoff_date) == datetime.date:
		cutoff_date = cutoff_date.timetuple()
	files = subprocess.check_output(["find", data_dir]).split()
	delete_list = []
	for f in files:
		parse_f = f.replace(".csv","")
		parse_f = parse_f.replace(".zip","")

		# files are either in xxxYYMMDD.xxx OR xxxYYMMDD_xx.xxx
		date_str = ""
		if re.search(".*_[0-9]{6}$",parse_f):
			date_str = parse_f[-6:]
		elif re.search(".*[0-9]{6}_[0-9]{2}$",parse_f):
			date_str = parse_f[-9:-3]
		if date_str:
			file_date = time.strptime(date_str,"%y%m%d")
			if file_date < cutoff_date:
				delete_list += [f]
	for w in delete_list:
		print w
		try:
			subprocess.call(["rm", w])
		except:
			print "error deleteing %s"%w
		
	
	


if __name__ == "__main__":
        con,cur = mysql_login.mysql_login()
	partitionDrop(cur)	
	drop_raw_files()
	if (con):
		con.commit()
		con.close()

