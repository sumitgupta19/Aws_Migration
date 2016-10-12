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
Convert(VARCHAR(24),EffectiveFrom),
Convert(VARCHAR(24),EffectiveTo)
from edw.tblofficedim_legacy where officedimkey between 200 and 10000 order by 1