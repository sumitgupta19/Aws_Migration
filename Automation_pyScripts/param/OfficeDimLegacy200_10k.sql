select 
OfficeDimKey,
OfficeID,
OfficeName,
DataSourceID,
SourceAliasID,
Address,
City,
State,
PostalCode,
Phone,
Email,
Convert(VARCHAR(24),EffectiveFrom,120),
Convert(VARCHAR(24),EffectiveTo,120)
from edw.tblofficedim_legacy where officedimkey between 200 and 10000 order by 1