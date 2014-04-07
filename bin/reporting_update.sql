USE DWA_SF_Cookie;
DROP TABLE IF EXISTS Vormetric_Reporting;
create table Vormetric_Reporting (
	`Date` DATE, 
	Clicks INT NOT NULL DEFAULT 0, 
	Impressions INT NOT NULL DEFAULT 0, 
	CampaignID MEDIUMINT, 
	CampaignName VARCHAR(255),
	SiteID INT, 
	SiteName VARCHAR(255),
	PlacementID INT, 
	AdvertiserID MediumINT
);

# make stripped down id version of table, then add named columns.
drop table if exists tmp2;
create temporary table tmp2 (
	d DATE, 
	CampaignID MEDIUMINT, 
	SiteID INT, 
	PlacementID INT, 
	AdvertiserID MediumINT
);

INSERT INTO tmp2 (d, CampaignID, SiteID, PlacementID, AdvertiserID) 
SELECT
Date(EventDate) d, 
CampaignID,
SIteID,
PlacementID,
AdvertiserID 
FROM Std_Vormetric GROUP BY
d, 
CampaignID,
SIteID,
PlacementID,
AdvertiserID 

;


INSERT INTO Vormetric_Reporting (`Date`, CampaignID, CampaignName, SiteID, SiteName, PLacementID, AdvertiserID) 
	SELECT  
	std.d,  
	std.CampaignID,
	c.CampaignName, 
	std.SiteID,
	s.sitename,
	std.PlacementID,
	std.AdvertiserID
FROM tmp2 std 
JOIN SF_Match.DisplayCampaigns c ON std.campaignID = c.campaignID
JOIN SF_Match.Sites s ON std.siteID = s.siteID




GROUP BY d, CampaignID, SIteID, PlacementID, AdvertiserID ORDER BY d;





UPDATE  Vormetric_Reporting vr JOIN 
(SELECT CampaignID, SiteID, PlacementID, AdvertiserID, date(EventDate) d, COUNT(*) clicks FROM Std_Vormetric WHERE eventTypeID = 2 GROUP BY 
CampaignID, SiteID, PlacementID, AdvertiserID,d ) tmp 
ON tmp.d = vr.`Date` 
AND vr.campaignID = tmp.campaignID
AND vr.SiteID = tmp.siteID
AND vr.placementID = tmp.placementID
AND vr.AdvertiserID = tmp.AdvertiserID

SET vr.Clicks = tmp.clicks
;


UPDATE  Vormetric_Reporting vr JOIN 
(SELECT CampaignID, SiteID, PlacementID, AdvertiserID, date(EventDate) d, COUNT(*) imps FROM Std_Vormetric WHERE eventTypeID = 1 GROUP BY 
CampaignID, SiteID, PlacementID, AdvertiserID,d ) tmp 
ON tmp.d = vr.`Date` 
AND vr.campaignID = tmp.campaignID
AND vr.SiteID = tmp.siteID
AND vr.placementID = tmp.placementID
AND vr.AdvertiserID = tmp.AdvertiserID

SET vr.Impressions = tmp.imps
;

SELECT * FROM Vormetric_Reporting;

select sum(Impressions) FROM vormetric_Reporting;

