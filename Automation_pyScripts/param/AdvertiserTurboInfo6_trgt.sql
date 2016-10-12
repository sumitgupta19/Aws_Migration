Select
AdvertiserTurboInfoID
,AdvertiserId
,weight_min
,weight_max
,StateCode
,City
,Convert(VARCHAR(24),CreatedDate)
,CreatedBy
,Convert(VARCHAR(24),ModifiedDate)
,ModifiedBy
,Convert(VARCHAR(24),EffectiveFrom)
,Convert(VARCHAR(24),EffectiveTo)
from edw.tblAdvertiserTurboInfo where AdvertiserTurboInfoID between 25001 and 30000 order by 1