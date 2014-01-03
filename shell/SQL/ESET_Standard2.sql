use DWA_SF_Cookie;

drop table if exists Z_Q4FY13_Imp;

Create table Z_Q4FY13_Imp
(
Select UserID from X_ESET_Standard
where CampaignID in (357107,361510)
and EventTypeID = 1
group by UserID
)
;
# 6,634,187 rows (distinct users)

# add an index
ALTER TABLE Z_Q4FY13_Imp ADD INDEX UserID (UserID);

# adding a column to highlight clickers
Alter table Z_Q4FY13_Imp add click varchar (1);

#populate the clickers
update Z_Q4FY13_Imp A
join 
(
Select distinct UserID from X_ESET_Standard
where CampaignID in (357107,361510)
and EventTypeID = 2   
) B
on A.UserID = B.UserID
set click = '1'
;


create table ESET_User_times as
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
where A.ConversionTagID = 164880 and C.click is null  # those that hit the homepage & those users who didn't click on an ad
order by A.UserID, A.ConversionDate
)
;