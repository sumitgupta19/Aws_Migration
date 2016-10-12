select
EventTypeCategoryDimKey
,SummaryCategory
,DetailCategory
,IsNull(Convert(VARCHAR(24),EffectiveFrom,120),'Null')
,IsNull(Convert(VARCHAR(24),EffectiveTo,120),'Null')
,EventClass
from Marketing.tblEventTypeCategoryDim
where EventTypeCategoryDimKey between -1 and 70
order by 1
