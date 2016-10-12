select
EventTypeCategoryDimKey
,SummaryCategory
,DetailCategory
,IsNull(Convert(VARCHAR(24),EffectiveFrom),'Null')
,IsNull(Convert(VARCHAR(24),EffectiveTo),'Null')
,EventClass
from Marketing.tblEventTypeCategoryDim
where EventTypeCategoryDimKey between -1 and 70
order by 1
