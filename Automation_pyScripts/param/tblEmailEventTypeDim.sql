Select 
EmailEventTypeDimKey
,Email_EventTypeID
,EventName
,Action
,EventTypeCategory
,IsNull(Convert(VARCHAR(24),EffectiveFrom,120),'Null')
,IsNull(Convert(VARCHAR(24),EffectiveTo,120),'Null')
from Marketing.tblEmailEventTypeDim
where EmailEventTypeDimKey between -1 and 50
order by 1