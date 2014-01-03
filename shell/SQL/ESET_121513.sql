use DWA_SF_Cookie;

drop table if exists Junk_ESET_times;

create table Junk_ESET_times as
(
Select  A.UserID, A.ConversionDate as HomepageDate, B.EventDate as ImpDate, Timediff(A.ConversionDate, B.EventDate) as TimeDiffr
from X_ESET_Conversion A
join
(
select UserID, EventDate from X_ESET_Standard 
where CampaignID in (357107,361510) and EventTypeID = 1 #user who received an impression from these specific campaigns
)  B
on A.userID = B.userID
join Z_Q4FY13_Imp C
on A.userID = C.userID
where A.ConversionTagID = 164880 and C.click is null  # those that hit the homepage, but didn't click on an ad
order by A.UserID, A.ConversionDate
)
;