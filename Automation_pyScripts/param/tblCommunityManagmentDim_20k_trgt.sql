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
,PushReports 
,Convert(VARCHAR(24),CreateDate) 
,Convert(VARCHAR(24),LastUpdated) 
,Convert(VARCHAR(24),ModifiedDate) 
,CreatedBy
,ModifiedBy
,HSPSCUSTOMERID
,SourceSystem
,SourceSystemType
,Convert(VARCHAR(24),EffectiveFrom) 
,Convert(VARCHAR(24),EffectiveTo) 
from edw.tblCommunityManagmentDim
where CommunityManagmentDimKey between 1015001 and 1020000