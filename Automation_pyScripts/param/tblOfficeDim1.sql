select
OfficeDimKey
,ROW_UID_MIN
,OfficeDataSourceID
,OfficeSourceAliasID
,OfficeID
,OfficeName
,OfficeAddress1
,OfficeAddress2
,IsNull(OfficeCity,'No OfficeCity')
,OfficeCounty
,OfficeStateCode
,OfficeStateName
,OfficeStateRegion
,OfficePostalCode
,IsNull(OfficePhoneNum,'No OfficePhoneNum')
,OfficeFaxNum
,OfficeCountryCode
,OfficeCountryName
,OfficeCountryRegion
,OfficeEmailAddress
,OfficeWebAddress
,OfficeAutoRoutingCode
,OfficeAddressLatitude
,OfficeAddressLongitude
,OfficePrimaryContactName
,OfficeStatus
,OfficeCurrentFlag
,IsNull(HLC,99999999)
,IsNull(CLC,99999999)
,convert(VARCHAR(24),EffectiveFrom,120)
,convert(VARCHAR(24),EffectiveTo,120)
,IsNull(HLCTier,'No HLCTier')
,IsNull(LegacyRealtorID,'No LegacyRealtorID')
,IsNull(LegacyCustID,'No LegacyCustID')
,CASE WHEN IsAOLSyndication = 'True' THEN 1 WHEN IsAOLSyndication='False' THEN 0 ELSE IsAOLSyndication  END 
,CASE WHEN IsRDCSyndication = 'True' THEN 1 WHEN IsRDCSyndication='False' THEN 0 ELSE IsRDCSyndication  END 
,IsNull(OfficeBOB,'No OfficeBOB')
,IsNull(OfficeBOBTeam,'No OfficeBOBTeam')
,IsNull(OfficeBOBCurrentManager,'No OfficeBOBCurrentManager')
,IsNull(OfficeBOBCurrentOwner,'No OfficeBOBCurrentOwner')
,IsNull(OfficeMLSRestriction,'No OfficeMLSRestriction')
,IsNull(OfficeDoNotContact,'No OfficeDoNotContact')
,convert(VARCHAR(24),CreatedDate,120)
,IsNull(CreatedBy,'No CreatedBy')
,convert(VARCHAR(24),ModifiedDate,120)
,IsNull(Modifiedby,'No Modifiedby')
,CASE WHEN LatestFlag = 'True' THEN 1 WHEN LatestFlag='False' THEN 0 ELSE LatestFlag  END 
,IsNull(SiebelAcctID,'No SiebelAcctID')
,IsNull(BrokerSiebelAcctID,'No BrokerSiebelAcctID')
,IsNull(SalesForceAccountID,'No SalesForceAccountID')
from edw.tblOfficeDim
where OfficeDimKey between 13522434 and 13527434
order by 1