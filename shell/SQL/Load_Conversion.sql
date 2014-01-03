
# ------------------------------------------------------------------------------------------------------------------------
# This process is for loading "CONVERSION" Cookie files from Mediamind into the MySql database
# 1st step is to load CSV directly into the DB in a Varchar format. 'Conversion' seems to take approx 15 mins per million rows
# 2nd step is to move the data from that Varchar table into a correctly formatted table which we will query against
# 3rd step is to clear the Varchar Load table and start loading the next file
# ------------------------------------------------------------------------------------------------------------------------

# select * from MM_Conversion order by Load_ID desc limit 100
# select max(date(ConversionDate)) from MM_Conversion


# 1st step
USE DWA_SF_Cookie;
load data local infile 'xxxxCSVFILExxxx'
into table DWA_SF_Cookie.MM_Conversion_LOAD_Varchar
fields terminated by ','
#enclosed by '"'
lines terminated by '\n'
ignore 1 lines
(UserID, ConversionID, ConversionDate, ConversionTagID, AdvertiserID, AccountID, Revenue, Currency, Quantity, OrderID, Referrer, ProductID, ProductInfo, WinnerEntityID, EventTypeID, WinnerEventDate, PlacementID, SiteID, CampaignID, AdGroupID, BrandID, IP, CountryID, StateID, DMAID, CityID, ZipCode, AreaCode, OSCode, BrowserCode, String1, String2, String3, String4, String5, String6, String7, String8, String9, String10, String11, String12, String13, String14, String15, String16, String17, String18, String19, String20)
;


# 2nd step. Insert into structured table

SET autocommit=0;

Insert into MM_Conversion
(
UserID, ConversionID,
ConversionDate,
ConversionTagID, AdvertiserID, AccountID, Revenue, Currency, Quantity, OrderID, Referrer, ProductID, ProductInfo, WinnerEntityID, EventTypeID, 
WinnerEventDate,
PlacementID, SiteID, CampaignID, AdGroupID, BrandID, IP, CountryID, StateID, DMAID, CityID, ZipCode, AreaCode, OSCode, BrowserCode,
String1,String2,String3,String4,String5,String6,String7,String8,String9,String10,String11,String12,String13,String14,String15,String16,String17,String18,String19,String20
)
select 
UserID, ConversionID,
STR_TO_DATE(ConversionDate, '%m/%d/%Y %h:%i:%s %p'),  
ConversionTagID, AdvertiserID, AccountID, Revenue, Currency, Quantity, OrderID, Referrer, ProductID, ProductInfo, WinnerEntityID, EventTypeID, 
STR_TO_DATE(WinnerEventDate, '%m/%d/%Y %h:%i:%s %p'),  
PlacementID, SiteID, CampaignID, AdGroupID, BrandID, IP, CountryID, StateID, DMAID, CityID, ZipCode, AreaCode, OSCode, BrowserCode,
String1, String2, String3, String4, String5, String6, String7, String8, String9, String10, String11, String12, String13, String14, String15,
String16, String17, String18, String19, String20
from MM_Conversion_LOAD_Varchar
;

COMMIT;

# 3rd step. Clear the 'LOAD_Varchar' table by drop and re-create, so it is ready to accept another source file

Drop table MM_Conversion_LOAD_Varchar;

create table MM_Conversion_LOAD_Varchar
(
UserID	varchar (255),
ConversionID	varchar (255),
ConversionDate	varchar (255),
ConversionTagID	varchar (255),
AdvertiserID	varchar (255),
AccountID	varchar (255),
Revenue	varchar (255),
Currency	varchar (255),
Quantity	varchar (255),
OrderID	varchar (255),
Referrer	varchar (10000),
ProductID	varchar (255),
ProductInfo	varchar (255),
WinnerEntityID	varchar (255),
EventTypeID	varchar (255),
WinnerEventDate	varchar (255),
PlacementID	varchar (255),
SiteID	varchar (255),
CampaignID	varchar (255),
AdGroupID	varchar (255),
BrandID	varchar (255),
IP	varchar (255),
CountryID	varchar (255),
StateID	varchar (255),
DMAID	varchar (255),
CityID	varchar (255),
ZipCode	varchar (255),
AreaCode	varchar (255),
OSCode	varchar (255),
BrowserCode	varchar (255),
String1	varchar (255) null,
String2	varchar (255) null,
String3	varchar (255) null,
String4	varchar (255) null,
String5	varchar (255) null,
String6	varchar (255) null,
String7	varchar (255) null,
String8	varchar (255) null,
String9	varchar (255) null,
String10	varchar (255) null,
String11	varchar (255) null,
String12	varchar (255) null,
String13	varchar (255) null,
String14	varchar (255) null,
String15	varchar (255) null,
String16	varchar (255) null,
String17	varchar (255) null,
String18	varchar (255) null,
String19	varchar (255) null,
String20	varchar (255) null
);



# ------------------------------------------------------------------------------------------------------------------------
# 4th step. Now go back and re-name the next load file as "MM_CLD_Standard_Agency_39792_Daily.csv" and start the process again
# ------------------------------------------------------------------------------------------------------------------------

#select date(EventDate), count(*) from MM_Standard group by date(EventDate)