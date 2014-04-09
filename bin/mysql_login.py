#!/usr/bin/python 
# -*- coding: utf-8 -*- 
import MySQLdb as mdb
import sys
import os
import re
import getopt
import datetime
import time
from warnings import filterwarnings

def db_connect(user,pw,db="",host="localhost"):

# get db handle: 
        try:
		if db == "":
                	con = mdb.connect(host, user, pw)
		else: 
			con = mdb.connect(host, user, pw, db)
                cur = con.cursor()
                return con,cur
        except mdb.Error, e:
                print "Error %d: %s" % (e.args[0], e.args[1])
                sys.exit(1)


def mysql_login():
        args = sys.argv[1:]
        optlist, args = getopt.getopt(args, 'u:h:d:p:')
        host="localhost"
        db=""

        for o in optlist:
                if o[0] == "-h":
                        host = o[1]
                elif o[0] == "-d":
                        db = o[1]
                elif o[0] == "-u":
                        user = o[1]
                elif o[0] == "-p":
                        pw = o[1]
        con,cur = db_connect(user,pw,db,host)
        return con,cur
