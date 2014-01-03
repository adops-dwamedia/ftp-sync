use DWA_SF_Cookie;

drop table if exists DWA_SF_Cookie.X_ESET_Standard;

Create table DWA_SF_Cookie.X_ESET_Standard as
select * from DWA_SF_Cookie.MM_Standard
where AdvertiserID = 61409;

ALTER TABLE DWA_SF_Cookie.X_ESET_Standard ADD INDEX EventTypeID (EventTypeID);
ALTER TABLE DWA_SF_Cookie.X_ESET_Standard ADD INDEX EventDate (EventDate);
ALTER TABLE DWA_SF_Cookie.X_ESET_Standard ADD INDEX UserID (UserID);
ALTER TABLE DWA_SF_Cookie.X_ESET_Standard ADD INDEX CampaignID (CampaignID);
ALTER TABLE DWA_SF_Cookie.X_ESET_Standard ADD INDEX SiteID (SiteID);
ALTER TABLE DWA_SF_Cookie.X_ESET_Standard ADD INDEX PlacementID (PlacementID);