#
#mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "ALTER TABLE MM_Standard ENGINE MyISAM"
#mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "ALTER IGNORE TABLE MM_Standard ADD UNIQUE INDEX(UserID, EventID, EventDate);"

#mysql -utomb -pDW4mediatb DWA_SF_Cookie -e "ALTER IGNORE TABLE MM_Standard ENGINE InnoDB"
#bash /usr/local/ftp_sync/bin/ftpImportNoRM.sh



echo "script complete"
		
