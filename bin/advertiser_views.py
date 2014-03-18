#!/usr/bin/python 
# -*- coding: utf-8 -*- 
import MySQLdb as mdb
import sys
import os
import re
import getopt
from warnings import filterwarnings

filterwarnings('ignore', category = mdb.Warning)





# script creates views for each advertiser, grouping all campaign ID's under the specific advertiser.

def db_connect(user,pw,db="",host="localhost"):

# get db handle: 
        try:
                con = mdb.connect(host, user, pw, db)
                cur = con.cursor()
                return con,cur
        except mdb.Error, e:
                print "Error %d: %s" % (e.args[0], e.args[1])
                sys.exit(1)

def get_campaign_advertisers(cur):
	# assumes that SF_Match exists and is accurate.
	cur.execute("USE SF_Match")

	# gather data. Brand ID's are in Campaign table, so we must link with campaign -> brand -> advertiser
	cur.execute(
	"SELECT jj.AdvertiserName, c.CampaignID FROM " \
	"DisplayCampaigns c JOIN " \
	"	(SELECT b.BrandID,  a.AdvertiserName FROM " \
	"		Brands b JOIN " \
	"		Advertisers a ON " \
	"		b.advertiserID = a.advertiserID) jj " \
	"ON jj.brandID = c.brandID;")
	raw= cur.fetchall()
	# convert to dict. key is advertiser, value is list of campaignIDs
	advertisers = {}
	for r in raw:
		adv, camp = r
		if adv not in advertisers.keys():
			advertisers[adv] = [camp]
		else:
			advertisers[adv].append(camp)
	return advertisers

def create_advertiser_views(advertisers, cur):
#	cur.execute("CREATE DATABASE IF NOT EXISTS advertiser_views")
#	cur.execute("USE advertiser_views")

	for a, cs in advertisers.iteritems():
		tbl_name = a.replace(" ", "_")
		cur.execute("DROP VIEW IF EXISTS %s"%tbl_name)	
		stmt = "CREATE VIEW %s AS SELECT * FROM DWA_SPOTLIGHT.dwa WHERE mm_CampaignID IN ("%tbl_name
		for c in cs:
			stmt += c+","
		stmt = stmt[:-1] + ")"
		cur.execute(stmt)
	
def main():
	con,cur = db_connect("tomb","DW4mediatb")
	advertisers = get_campaign_advertisers(cur)
	create_advertiser_views(advertisers,cur)

