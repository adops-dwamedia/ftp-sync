#mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "ALTER TABLE MM_Standard ENGINE MyISAM"
#mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "ALTER IGNORE TABLE MM_Standard ADD UNIQUE INDEX(UserID, EventID, EventDate);"

#mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "ALTER IGNORE TABLE MM_Standard ENGINE InnoDB"
#bash /usr/local/ftp_sync/bin/ftpImport.sh
#mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "DROP TABLE IF EXISTS MM_Standard_P"
mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "DROP TABLE IF EXISTS MM_Standard_P;CREATE TABLE MM_Standard_P AS SELECT UserID, EventID, EventTypeID, EventDate, CampaignID, SiteID, PlacementID, IP, AdvertiserID FROM MM_Standard WHERE 1=0"
mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "ALTER TABLE MM_Standard_P ADD PRIMARY KEY (UserID, EventID, EventTypeID, EventDate, CampaignID, SiteID, PlacementID, IP, AdvertiserID)"
exit
mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "ALTER TABLE MM_Standard_P PARTITION BY LIST COLUMNS (AdvertiserID)(
PARTITION p01 VALUES IN (59353),
PARTITION p02 VALUES IN (60687),
PARTITION p03 VALUES IN (61253),
PARTITION p04 VALUES IN (61409),
PARTITION p05 VALUES IN (61411),
PARTITION p06 VALUES IN (61788),
PARTITION p07 VALUES IN (61903),
PARTITION p08 VALUES IN (67963),
PARTITION p09 VALUES IN (69155),
PARTITION p10 VALUES IN (72663),
PARTITION p11 VALUES IN (75004),
PARTITION p12 VALUES IN (80301),
PARTITION p13 VALUES IN (85951),
PARTITION p14 VALUES IN (89647),
PARTITION p15 VALUES IN (92381),
PARTITION p16 VALUES IN (93593),
PARTITION p17 VALUES IN (95500),
PARTITION p18 VALUES IN (97294));"



echo "script complete"
		
