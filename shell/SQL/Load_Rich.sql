
# ------------------------------------------------------------------------------------------------------------------------
# This process is for loading "RICH" Cookie files from Mediamind into the MySql database
# 1st step is to load CSV directly into the DB in a Varchar format. 
# 2nd step is to move the data from that Varchar table into a correctly formatted table which we will query against
# 3rd step is to clear the Varchar Load table and start loading the next file
# ------------------------------------------------------------------------------------------------------------------------

# select * from MM_Rich order by Load_ID desc limit 100
# select max(date(InteractionDate)) from MM_Rich

# 1st step
load data local infile @source 
into table DWA_SF_Cookie.MM_Rich_LOAD_Varchar
fields terminated by ','
#enclosed by '"'
lines terminated by '\n'
ignore 1 lines
(EventID, UserID, EventTypeID, InteractionID, InteractionDuration, VideoAssetID, InteractionDate, EntityID, PlacementID, SiteID, CampaignID, BrandID, AdvertiserID, AccountID, PCP)
;


# 2nd step. Insert into structured table

SET autocommit=0;

Insert into MM_Rich
(
EventID, UserID, EventTypeID, InteractionID, InteractionDuration, VideoAssetID,
InteractionDate,
EntityID, PlacementID, SiteID, CampaignID, BrandID, AdvertiserID, AccountID, PCP
)
select 
EventID, UserID, EventTypeID, InteractionID, InteractionDuration, VideoAssetID,
STR_TO_DATE(InteractionDate, '%m/%d/%Y %h:%i:%s %p'),
EntityID, PlacementID, SiteID, CampaignID, BrandID, AdvertiserID, AccountID, PCP
from MM_Rich_LOAD_Varchar
;

COMMIT;

# 3rd step. Clear the 'LOAD_Varchar' table by drop and re-create, so it is ready to accept another source file

Drop table MM_Rich_LOAD_Varchar;

create table MM_Rich_LOAD_Varchar
(
EventID varchar (255),
UserID varchar (255),
EventTypeID varchar (255),
InteractionID varchar (255),
InteractionDuration varchar (255),
VideoAssetID varchar (255),
InteractionDate varchar (255),
EntityID varchar (255),
PlacementID varchar (255),
SiteID varchar (255),
CampaignID varchar (255),
BrandID varchar (255),
AdvertiserID varchar (255),
AccountID varchar (255),
PCP varchar (255)
);



# ------------------------------------------------------------------------------------------------------------------------
# 4th step. Now go back and re-name the next load file as "MM_CLD_Standard_Agency_39792_Daily.csv" and start the process again
# ------------------------------------------------------------------------------------------------------------------------

#select date(InteractionDate), count(*) from MM_Rich group by date(InteractionDate)
