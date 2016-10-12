SELECT 
SortKey
,MLSID
,ListingOfficeMLSID
,ListingOfficeName
,ListingStatus
,BOP
,ListingAgentID
,ListingAgentName
,MLSPropertyID
,PropertyStreetAddress
,PropertyCity
,PropertyState
,PropertyZIP
,PropertyPrice
,PhotoCount
,RDCSearchResultViews
,AOLSearchResultViews
,RDCListingsDetailPageviews
,AOLListingsDetailPageviews
,RDCTransferToAgentOfficeWebsites
,AOLTransferToAgentOfficeWebsites
,RDCCustomerClickAnimSignReader
,AOLCustomerClickAnimSignReader
,ReportFrequency
,Datadate
FROM Report.tblListingBCPReportExportOutPut
WHERE SortKey BETWEEN 1 and 762