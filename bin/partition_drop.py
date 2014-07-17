#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import mysql_login
import time
from warnings import filterwarnings
filterwarnings('ignore', category = mdb.Warning)        





def partitionDrop(d,db,cur):
	cur.execute("USE %s"%db)

	cur.execute("SHOW TABLES")

	tbls = [t[0] for t in cur.fetchall() if t[0] in [ "MM_Rich"]]

	for t in tbls:
		print t
		cur.execute("EXPLAIN PARTITIONS SELECT * FROM %s"%t)
		parts = cur.fetchall()[0][3]
		if parts is not None:
			parts_d = {p:time.strptime(p[1:],"%Y-%m-%d")  for p in parts.split(",") if p != "pMAX"}

			parts_d = {k:parts_d[k] for k in parts_d if parts_d[k] < d }
			print parts_d.keys()

			for k in parts_d.keys():
				stmt = "alter table %s drop partition `%s`"%(t, k)
				print stmt
				cur.execute(stmt)




if __name__ == "__main__":
        con,cur = mysql_login.mysql_login()
	partitionDrop(time.strptime("2014-07-01", "%Y-%m-%d"),"DWA_SF_Cookie",cur)	

	if (con):
		con.commit()
		con.close()
