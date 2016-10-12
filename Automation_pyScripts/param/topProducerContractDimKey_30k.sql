SELECT 
topProducerContractDimKey
,OwnerId
,CustomerStatus
,BID
,FirstName
,LastName
,EmailAddress
,Address1
,City
,RegionCode
,CountryCode
,PostCode
,PhoneNumber
,CompanyName
,OwnsCRM
,Convert(VARCHAR(24),CRMStartDate,120) 
,Convert(VARCHAR(24),CRMClosureDate,120) 
,OwnsMS
,Convert(VARCHAR(24),MSStartDate,120) 
,Convert(VARCHAR(24),MSClosureDate,120) 
,OwnsMB
,Convert(VARCHAR(24),MBStartDate,120) 
,Convert(VARCHAR(24),MBClosureDate,120) 
,OwnsTWS
,Convert(VARCHAR(24),TWSStartDate,120) 
,Convert(VARCHAR(24),TWSClosureDate,120) 
,OwnsIDX
,Convert(VARCHAR(24),IDXStartDate,120) 
,Convert(VARCHAR(24),IDXClosureDate,120) 
,OwnsBlog
,Convert(VARCHAR(24),BlogStartDate,120)
,Convert(VARCHAR(24),BlogClosureDate,120)
,MLSID
,MLSAlias
,RealtorTypeID
,SiebelAccountID
,CAST(MonthlyValue as decimal(38,6))
,OwnsJLP
,Convert(VARCHAR(24),JLPStartDate,120)
,Convert(VARCHAR(24),JLPClosureDate,120)
,IsCanadian
,Convert(VARCHAR(24),InsertDate,120) 
,Convert(VARCHAR(24),UpdateDate,120) 
,ChangeCheck
,DeleteFlag
from edw.tblTopProducerContractDim
where topProducerContractDimKey between 25001 and 30000