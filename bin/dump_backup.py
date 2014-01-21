import os
import MySQLdb as mdb
import sys
from warnings import filterwarnings


filterwarnings('ignore', category = mdb.Warning)

host = 'localhost'
user = 'root'
pw = 'tolley'

ib_fileName = "/var/lib/mysql/ibdata1"
ib_log0 = "/var/lib/mysql/ib_logfile0"
ib_log1 = "/var/lib/mysql/ib_logfile1"

# Create dump file
bashCmd = "mysqldump -u%s -p%s --all-databases > dump.sql"%(user,pw)
os.system(bashCmd)


# get db handle:
try:
	con = mdb.connect(host, user, pw)
	cur = con.cursor()

	
except mdb.Error, e:

	print "Error %d: %s" % (e.args[0], e.args[1])
	sys.exit(1)
cur.execute("SHOW DATABASES")
dbs = cur.fetchall()
for db_t in dbs:
	db = db_t[0]
	if db not in ["information_schema", "performance_schema", "mysql"]:
		stmt = "DROP DATABASE %s"%db
		cur.execute(stmt)

bashCmd = "service mysql stop;"
bashCmd += "rm %s; rm %s; rm %s;"%(ib_fileName, ib_log0, ib_log1)
bashCmd += " service mysql start;"
bashCmd += "mysql -u%s -p%s < dump.sql"
print bashCmd

os.system(bashCmd)



if con:
	con.close()

