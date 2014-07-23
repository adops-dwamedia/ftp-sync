#############
Cookie Update
#############

Overview

A python program to download raw data from an FTP server.

Much of the code is tailored specifically to MediaMind (Sizmek)'s file formats.

In theory, it should be possible to drop this code on any Unix server and run it.
It is supposed to be able to set up all of its necessary folders and databases.

Files:

cookie_update.py	-	The "main" file, which is the only one that needs to be invoked.
mysql_login.py		-	A file for creating MySQL con and cur objects, and allows the 
				Python script to be invoked with credentials in the same way 
				as MySQL, ie cookie_update -uUSER -pPASSWORD
cleanup.py		-	Drops old partitions, removes old raw source files.

#############

Databases

Two databases are created, DWA_SF_Cookie and SF_Match.  DWA_SF_Cookie holds
primary data, and the SF_Match_ table holds match tables for the various ID's 
involved.

The tables of DWA_SF_Cookie:

- A collection of Std_Client tables - Contain data for clicks and impressions.

- MM_Rich table: Information about rich media impressions

- MM_Conversion: Information about conversions

- exclude_list: tracks which files have already been processed, and are thus safe to ignore.

- MM_Standard: A view which combines all of the Std_Client tables into one.


#############

Invocation

The entire program, from initialization to everyday maintenance, is run by calling the following:

python cookie_update.py -uUSER -pPASSWORD

Ideally, this program is scheduled to run once a day. This is scheduling can be edited with the
crontab -e command.



