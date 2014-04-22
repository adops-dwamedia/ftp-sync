#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import subprocess
import sys
import os
import re
import datetime
import mysql_login
import cookie_update
import attribution2
import conversions


from warnings import filterwarnings
filterwarnings('ignore', category = mdb.Warning)


cookie_update.main()
attribution2.main()
