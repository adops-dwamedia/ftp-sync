LOAD DATA LOCAL INFILE 'test.csv'
into table DWA_SF_Cookie.MM_Standard_LOAD_Varchar
fields terminated by ','
#enclosed by '"'
lines terminated by '\n'
ignore 1 lines
(EventID, UserID, EventTypeID, EventDate, EntityID, PlacementID, SiteID, CampaignID, BrandID, AdvertiserID, AccountID, SearchAdID, AdGroupID, IP,CountryID, StateID, DMAID, CityID, ZipCode, AreaCode, BrowserCode, OSCode, Referrer, MobileDevice, MobileCarrier, AudienceID, ProductID, PCP)
;

# 2nd step. Insert into structured table

SET autocommit=0;

Insert into MM_Standard
(EventID, UserID, EventTypeID, 
EventDate, 
EntityID, PlacementID, SiteID, CampaignID, BrandID, AdvertiserID, AccountID, SearchAdID, AdGroupID, IP,CountryID, StateID, DMAID, CityID, ZipCode, AreaCode, BrowserCode, OSCode, Referrer, MobileDevice, MobileCarrier, AudienceID, ProductID, PCP)
select 
EventID, UserID, EventTypeID, 
STR_TO_DATE(EventDate, '%m/%d/%Y %h:%i:%s %p'),  
EntityID, PlacementID, SiteID, CampaignID, BrandID, AdvertiserID, AccountID, SearchAdID, AdGroupID, IP,CountryID, StateID, DMAID, CityID, ZipCode, AreaCode, BrowserCode, OSCode, Referrer, MobileDevice, MobileCarrier, AudienceID, ProductID, PCP
FROM MM_Standard_LOAD_Varchar
;

COMMIT;

# 3rd step. Clear the 'LOAD_Varchar' table by drop and re-create, so it is ready to accept another source file

Drop table MM_Standard_LOAD_Varchar;

create table MM_Standard_LOAD_Varchar
(
EventID	varchar (255),
UserID	varchar (255),
EventTypeID	varchar (255),
EventDate	varchar (255),
EntityID	varchar (255),
PlacementID	varchar (255),
SiteID	varchar (255),
CampaignID	varchar (255),
BrandID	varchar (255),
AdvertiserID	varchar (255),
AccountID	varchar (255),
SearchAdID	varchar (255),
AdGroupID	varchar (255),
IP	varchar (255),
CountryID	varchar (255),
StateID	varchar (255),
DMAID	varchar (255),
CityID	varchar (255),
ZipCode	varchar (255),
AreaCode	varchar (255),
BrowserCode	varchar (255),
OSCode	varchar (255),
Referrer	varchar (10000),
MobileDevice	varchar (255),
MobileCarrier	varchar (255),
AudienceID	varchar (255),
ProductID	varchar (255),
PCP	varchar (255)
);

# ------------------------------------------------------------------------------------------------------------------------
# 4th step. Now go back and re-name the next load file as "MM_CLD_Standard_Agency_39792_Daily.csv" and start the process again
# ------------------------------------------------------------------------------------------------------------------------

