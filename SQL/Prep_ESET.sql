# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# We need to do this every day we run queries as we get new data loaded daily
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
#select count(*) from X_ESET_Standard
# 1st step. We're going extract data from MM_Standard, for ESET
drop table DWA_SF_Cookie.X_ESET_Standard;

Create table DWA_SF_Cookie.X_ESET_Standard as
select * from DWA_SF_Cookie.MM_Standard
where AdvertiserID = 61409;

# Adding indexes on what we'll query against
ALTER TABLE DWA_SF_Cookie.X_ESET_Standard ADD INDEX EventTypeID (EventTypeID);
ALTER TABLE DWA_SF_Cookie.X_ESET_Standard ADD INDEX EventDate (EventDate);
ALTER TABLE DWA_SF_Cookie.X_ESET_Standard ADD INDEX UserID (UserID);
ALTER TABLE DWA_SF_Cookie.X_ESET_Standard ADD INDEX CampaignID (CampaignID);
ALTER TABLE DWA_SF_Cookie.X_ESET_Standard ADD INDEX SiteID (SiteID);
ALTER TABLE DWA_SF_Cookie.X_ESET_Standard ADD INDEX PlacementID (PlacementID);

# 2nd step. We're going extract data from MM_Conversion, for ESET
drop table DWA_SF_Cookie.X_ESET_Conversion;

Create table DWA_SF_Cookie.X_ESET_Conversion as
select * from DWA_SF_Cookie.MM_Conversion
where AdvertiserID = 61409;

# Adding indexes on what we'll query against
ALTER TABLE DWA_SF_Cookie.X_ESET_Conversion ADD INDEX UserID (UserID);
ALTER TABLE DWA_SF_Cookie.X_ESET_Conversion ADD INDEX ConversionTagID (ConversionTagID);
ALTER TABLE DWA_SF_Cookie.X_ESET_Conversion ADD INDEX ConversionDate (ConversionDate);

# 3rd step. We're going extract data from MM_Rich, for ESET
drop table DWA_SF_Cookie.X_ESET_Rich;

Create table DWA_SF_Cookie.X_ESET_Rich as
select * from DWA_SF_Cookie.MM_Rich
where AdvertiserID = 61409;

# Adding indexes on what we'll query against
ALTER TABLE DWA_SF_Cookie.X_ESET_Rich ADD INDEX UserID (UserID);
ALTER TABLE DWA_SF_Cookie.X_ESET_Rich ADD INDEX EventTypeID (EventTypeID);
ALTER TABLE DWA_SF_Cookie.X_ESET_Rich ADD INDEX InteractionDate (InteractionDate);
ALTER TABLE DWA_SF_Cookie.X_ESET_Rich ADD INDEX CampaignID (CampaignID);
ALTER TABLE DWA_SF_Cookie.X_ESET_Rich ADD INDEX SiteID (SiteID);
ALTER TABLE DWA_SF_Cookie.X_ESET_Rich ADD INDEX PlacementID (PlacementID);
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

