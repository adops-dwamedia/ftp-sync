use DWA_SF_Cookie;

create table Junk_ESET_times_Thx as
(
Select  A.UserID, A.ConversionTagID, A.ConversionDate as ThanksDate, B.EventDate as ImpDate, Timediff(A.ConversionDate, B.EventDate) as TimeDiffr
from X_ESET_Conversion A				
join Z_Q4FY13_Imp B					# Z_Q4FY13_Imp table - User received an Imp or click in the specific campaigns
on A.userID = B.userID
where B.click is null  					# but didn't click on an ad
and A.ConversionTagID in ('163398','163399','163400','164460','164463','238359','313737','313738','313739','355170','355174','355175')
order by A.UserID, A.ConversionDate
)													
;

