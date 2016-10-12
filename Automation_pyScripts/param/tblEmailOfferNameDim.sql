select 
OfferDimKey
,OfferName
,OfferCategory
,IsNull(Convert(VARCHAR(24),EffectiveFrom,120),'Null')
,IsNull(Convert(VARCHAR(24),EffectiveTo,120),'Null')
from marketing.tblEmailOfferNameDim
where OfferDimKey between -1 and 2169
order by 1