select 
CommunityDimKey
,PropID
,MgmtID
,ListingTypeID
,PropStatusID
,PropName
,Address1
,Address2
,PostalCode
,CAST(Latitude as decimal(38,6))
,CAST(Longitude as decimal(38,6))
,Convert(VARCHAR(24),CreateDate,120)  
,HSPSContractId
,HSPSCustomerId
,City
,State
,Phone
,Fax
,Email
,NumUnits
,ClientPropertyID
,ClientPeopleSoftID
,MultipleLeadTypeID
,Convert(VARCHAR(24),LastUpdated,120)   
,CreatedBy
,ModifiedBy
,FeedSource
,Convert(VARCHAR(24),Last_Updated_By_Datafeed,120) 
,ProductID
,IVRContactTypeID
,IvrDispLocal
,IvrWhisper
,AAHSAFlag
,ConfirmAllCalls
,Convert(VARCHAR(24),ModifiedDate,120) 
,ATT_TollFree
,CASE WHEN MLGFlag = 'True' THEN 1 ELSE 0 END  
,TFNAssignmentID
,SourceSystem
,SourceSystemType
,Convert(VARCHAR(24),EffectiveFrom,120) 
,Convert(VARCHAR(24),EffectiveTo,120) 
from edw.tblcommunitydim
where CommunityDimKey between 10001 and 15000