Select
AdvertiserTurboInfoID
,AdvertiserId
,CAST(weight_min as decimal(38,6))
,CAST(weight_max as decimal(38,6))
,StateCode
,City
,IsNull(Convert(VARCHAR(24),CreatedDate,120),'No CreatedDate')
,IsNull(CreatedBy,'No CreatedBy')
,IsNull(Convert(VARCHAR(24),ModifiedDate,120),'No ModifiedDate')
,IsNUll(ModifiedBy,'No ModifiedBy')
,Convert(VARCHAR(24),EffectiveFrom,120)
,Convert(VARCHAR(24),EffectiveTo,120)
from edw.tblAdvertiserTurboInfo where AdvertiserTurboInfoID between -1 and 5000 order by 1