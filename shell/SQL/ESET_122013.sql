use DWA_SF_Cookie;

# creating a table of UserID's for those who received an Imp & flag those who clicked
Create table Z_Q4FY13_Imp
(
Select UserID, EventDate from X_ESET_Standard
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
