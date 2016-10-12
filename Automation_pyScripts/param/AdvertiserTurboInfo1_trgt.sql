Select
AdvertiserTurboInfoID
,AdvertiserId
,weight_min
,weight_max
,StateCode
,City
,IsNull(Convert(VARCHAR(24),CreatedDate),'No CreatedDate')
,IsNull(CreatedBy,'No CreatedBy')
,IsNull(Convert(VARCHAR(24),ModifiedDate),'No ModifiedDate')
,IsNUll(ModifiedBy,'No ModifiedBy')
,Convert(VARCHAR(24),EffectiveFrom)
,Convert(VARCHAR(24),EffectiveTo)
from edw.tblAdvertiserTurboInfo where AdvertiserTurboInfoID between -1 and 5000 order by 1