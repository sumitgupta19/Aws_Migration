select 
OfferDimKey
,OfferName
,OfferCategory
,IsNull(Convert(VARCHAR(24),EffectiveFrom),'Null')
,IsNull(Convert(VARCHAR(24),EffectiveTo),'Null')
from marketing.tblEmailOfferNameDim
where OfferDimKey between -1 and 2169
order by 1