#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import mysql_login
from warnings import filterwarnings
filterwarnings('ignore', category = mdb.Warning)        





def partitionDrop(d):
	cur.execute("EXPLAIN PARTITIONS SELECT * FROM %s"%tblName)
	parts = cur.fetchall()[0][3]




if __name__ == "__main__":
        start = datetime.datetime.now()
        con,cur = mysql_login.mysql_login()
