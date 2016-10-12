select 
DataSourceDimKey
,DataSourceID
,DataSourceName
,DataSourceCode
,DataSourceMarketArea
,DataSourceMLSMapPage
,DataSourceDisplayWebSite
,DataSourceForeclosureOption
,IsNull(Convert(VARCHAR(24),EffectiveFrom),'Null') 
,IsNull(Convert(VARCHAR(24),EffectiveTo),'Null')
,DataSourceCopyrightInfo
from edw.tblDataSourceDim where datasourcedimkey between -1 and 1515
order by 1 