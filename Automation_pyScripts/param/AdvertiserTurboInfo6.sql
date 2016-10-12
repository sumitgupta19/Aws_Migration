Select
AdvertiserTurboInfoID
,AdvertiserId
,CAST(weight_min as decimal(38,6))
,CAST(weight_max as decimal(38,6))
,StateCode
,City
,Convert(VARCHAR(24),CreatedDate,120)
,CreatedBy
,Convert(VARCHAR(24),ModifiedDate,120)
,ModifiedBy
,Convert(VARCHAR(24),EffectiveFrom,120)
,Convert(VARCHAR(24),EffectiveTo,120)
from edw.tblAdvertiserTurboInfo where AdvertiserTurboInfoID between 25001 and 30000 order by 1