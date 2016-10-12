select 
DataSourceDimKey
,DataSourceID
,DataSourceName
,DataSourceCode
,DataSourceMarketArea
,DataSourceMLSMapPage
,DataSourceDisplayWebSite
,DataSourceForeclosureOption
,IsNull(Convert(VARCHAR(24),EffectiveFrom,120),'Null')
,IsNull(Convert(VARCHAR(24),EffectiveTo,120),'Null')
,DataSourceCopyrightInfo
from edw.tblDataSourceDim where datasourcedimkey between -1 and 1515
order by 1
