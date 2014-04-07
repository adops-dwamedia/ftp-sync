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
	cur.execute("CREATE DATABASE  IF NOT EXISTS reports")
def get_
def pull_report(cur,con,metrics,dimensions):
	
	
