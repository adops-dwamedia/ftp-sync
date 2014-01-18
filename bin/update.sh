#
#mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "ALTER TABLE MM_Standard ENGINE MyISAM"
#mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "ALTER IGNORE TABLE MM_Standard ADD UNIQUE INDEX(UserID, EventID, EventDate);"

#mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "ALTER IGNORE TABLE MM_Standard ENGINE InnoDB"
#bash /usr/local/ftp_sync/bin/ftpImportNoRM.sh
#mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "DROP TABLE IF EXISTS MM_Standard_P"
#mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "CREATE TABLE MM_Standard_P AS SELECT UserID, EventID, EventTypeID, EventDate, CampaignID, SiteID, PlacementID, IP, AdvertiserID FROM MM_Standard WHERE 1=0"
#mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "ALTER IGNORE TABLE MM_Standard_P ADD PRIMARY KEY (UserID, EventID, EventTypeID, EventDate, CampaignID, SiteID, PlacementID, IP, AdvertiserID)"
python partition.py
mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "INSERT IGNORE INTO MM_Standard_P SELECT UserID, EventID, EventTypeID, STR_TO_DATE(EventDate, '%m/%d/%Y %h:%i:%s %p'), CampaignID, SiteID, PlacementID, IP, AdvertiserID FROM MM_Standard"
python advertiserTables.py
echo "script complete"
		
