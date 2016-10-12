select 
CommunityManagmentDimKey
,MgmtID
,MgmtName
,MgmtAddress1
,MgmtAddress2
,MgmtCity
,MgmtState
,MgmtPostalCode
,MgmtContact
,BillingContact
,MgmtPhone
,MgmtFax
,MgmtEmail
,MgmtLicense
,MgmtHomePage
,MgmtLogo
,CASE WHEN PushReports = 'True' THEN 1 ELSE 0 END 
,Convert(VARCHAR(24),CreateDate,120) 
,Convert(VARCHAR(24),LastUpdated,120) 
,Convert(VARCHAR(24),ModifiedDate,120) 
,CreatedBy
,ModifiedBy
,HSPSCUSTOMERID
,SourceSystem
,SourceSystemType
,Convert(VARCHAR(24),EffectiveFrom,120) 
,Convert(VARCHAR(24),EffectiveTo,120) 
from edw.tblCommunityManagmentDim
where CommunityManagmentDimKey between 1010001 and 1015000