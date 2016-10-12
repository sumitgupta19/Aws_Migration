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
,convert(VARCHAR(24),EffectiveFrom)
,convert(VARCHAR(24),EffectiveTo)
,IsNull(HLCTier,'No HLCTier')
,IsNull(LegacyRealtorID,'No LegacyRealtorID')
,IsNull(LegacyCustID,'No LegacyCustID')
,IsAOLSyndication
,IsRDCSyndication
,IsNull(OfficeBOB,'No OfficeBOB')
,IsNull(OfficeBOBTeam,'No OfficeBOBTeam')
,IsNull(OfficeBOBCurrentManager,'No OfficeBOBCurrentManager')
,IsNull(OfficeBOBCurrentOwner,'No OfficeBOBCurrentOwner')
,IsNull(OfficeMLSRestriction,'No OfficeMLSRestriction')
,IsNull(OfficeDoNotContact,'No OfficeDoNotContact')
,convert(VARCHAR(24),CreatedDate)
,IsNull(CreatedBy,'No CreatedBy')
,convert(VARCHAR(24),ModifiedDate)
,IsNull(Modifiedby,'No Modifiedby')
,LatestFlag
,IsNull(SiebelAcctID,'No SiebelAcctID')
,IsNull(BrokerSiebelAcctID,'No BrokerSiebelAcctID')
,IsNull(SalesForceAccountID,'No SalesForceAccountID')
from edw.tblOfficeDim
where OfficeDimKey between 1000 and 6000
order by 1