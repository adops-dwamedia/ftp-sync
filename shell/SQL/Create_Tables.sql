
# Standard File
create table MM_Standard
(
Load_ID integer NOT NULL AUTO_INCREMENT,
EventID varchar (36),
UserID varchar (36),
EventTypeID integer,
EventDate datetime,
EntityID integer,
PlacementID varchar(11),
SiteID varchar(11), 
CampaignID varchar(11),
BrandID varchar(11), 
AdvertiserID integer,
AccountID integer,
SearchAdID varchar(15),
AdGroupID varchar(15),
IP varchar (15),
CountryID integer,
StateID integer, 
DMAID integer, 
CityID integer, 
ZipCode varchar (10), 
AreaCode varchar (5), 
BrowserCode integer, 
OSCode integer, 
Referrer TEXT, 
MobileDevice varchar (50), 
MobileCarrier varchar (10), 
AudienceID integer, 
ProductID varchar (11), 
PCP varchar (10),
PRIMARY KEY (Load_ID)
);


# Adding an 'index' 
# We're adding this index on "AdvertiserID", as we use this later, to extract the data from MM_Standard

ALTER TABLE DWA_SF_Cookie.MM_Standard ADD INDEX AdvertiserID (AdvertiserID)
;

# show indexes from MM_Standard
