use DWA_SF_Cookie;

select count(distinct A.UserID)
from Junk_ESET_times_Thx A
join X_ESET_Standard B
on A.UserID = B.UserID
where A.TimeDiffr >0 and TimeDiffr <= 240000
and B.EventID = 3
;