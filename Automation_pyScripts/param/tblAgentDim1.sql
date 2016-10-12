Select 
AgentDimKey
,IsNull(Convert(VARCHAR(24),OfficeDimKey),'Null')
,ROW_UID_MIN
,IsNull(AgentDataSourceID,'Null')
,AgentSourceAliasID
,OfficeDataSourceID
,OfficeSourceAliasID
,IsNull(Convert(VARCHAR(24),HLC),'Null')
,IsNull(Convert(VARCHAR(24),CLC),'Null')
,AgentID
,AgentName
,AgentAddress1
,AgentAddress2
,AgentCity
,AgentCounty
,AgentPostalCode
,AgentStateCode
,AgentStateName
,AgentStateRegion
,AgentCountryCode
,AgentCountryName
,AgentCountryRegion
,AgentPhoneNum
,AgentFaxNum
,AgentEmailAddress
,AgentWebAddress
,AgentAutoRoutingCode
,AgentAddressLatitude
,AgentAddressLongitude
,AgentPrimaryContactName
,AgentStatus
,AgentCurrentFlag
,Convert(VARCHAR(24),EffectiveFrom,120)
,Convert(VARCHAR(24),EffectiveTo,120)
,IsNull(HLCTier,'Null')
,IsNull(LegacyRealtorID,'Null')
,IsNull(LegacyCustID,'Null')
,IsNull(AgentCellPhone,'Null')
,IsNull(AgentSlogan,'Null')
,IsNull(AgentPhotoURL1,'Null')
,IsNull(AgentPhotoURL2,'Null')
,IsNull(AgentLinkedInURL,'Null')
,IsNull(AgentFacebookURL,'Null')
,IsNull(AgentTwitterURL,'Null')
,IsNull(Convert(VARCHAR(24),AgentSinceDate,120),'Null')
,IsNull(AgentBOB,'Null')
,IsNull(AgentBOBTeam,'Null')
,IsNull(AgentBOBCurrentManager,'Null')
,IsNull(AgentBOBCurrentOwner,'Null')
,IsNull(AgentMLSRestriction,'Null')
,IsNull(AgentDoNotContact,'Null')
,IsNull(AgentRosterEmailAddress,'Null')
,IsNull(AgentConsumerInquiryEmailAddress,'Null')
,IsNull(AgentCustomerEmailAddress,'Null')
,IsNull(AgentName_Uhura,'Null')
,IsNull(AgentEmail_Uhura,'Null')
,IsNull(AgentMobilePhone_Uhura,'Null')
,IsNull(AgentGenPhone_Uhura,'Null')
,IsNull(AgentFaxPhone_Uhura,'Null')
,IsNull(AgentTollFreePhone_Uhura,'Null')
,IsNull(AgentURL_Uhura,'Null')
,IsNull(AgentReasonCode,'Null')
,CASE WHEN IsAgentsStateLicensedSet = 'True' THEN 1 WHEN IsAgentsStateLicensedSet='False' THEN 0 ELSE IsAgentsStateLicensedSet  END 
,CASE WHEN IsAgentsCountryLicensedSet = 'True' THEN 1 WHEN IsAgentsCountryLicensedSet='False' THEN 0 ELSE IsAgentsCountryLicensedSet  END
, CASE WHEN IsAgentsMarketingAreaSet = 'True' THEN 1 WHEN IsAgentsMarketingAreaSet='False' THEN 0 ELSE IsAgentsMarketingAreaSet  END 
, CASE WHEN IsAgentsMarketingZipSet = 'True' THEN 1 WHEN IsAgentsMarketingZipSet='False' THEN 0 ELSE IsAgentsMarketingZipSet  END
,CASE WHEN IsAgentsDesignationSet = 'True' THEN 1 WHEN IsAgentsDesignationSet='False' THEN 0 ELSE IsAgentsDesignationSet  END 
,CASE WHEN IsAgentsKeywordSet = 'True' THEN 1 WHEN IsAgentsKeywordSet='False' THEN 0 ELSE IsAgentsKeywordSet  END 
,CASE WHEN IsAgentsAboutMeSet = 'True' THEN 1 WHEN IsAgentsAboutMeSet='False' THEN 0 ELSE IsAgentsAboutMeSet  END 
,CASE WHEN IsAgentsFirstYearOfServiceSet = 'True' THEN 1 WHEN IsAgentsFirstYearOfServiceSet='False' THEN 0 ELSE IsAgentsFirstYearOfServiceSet  END 
,CAST(AgentProfilePercentageCompleteness as decimal(38,6))
,IsNull(Convert(VARCHAR(24),CreatedDate,120),'Null')
,IsNull(CreatedBy,'Null')
,IsNull(Convert(VARCHAR(24),ModifiedDate,120),'Null')
,IsNull(Modifiedby,'Null')
,IsNull(Convert(VARCHAR(24),TeamDimKey),'Null')
,IsNull(AgentNRDSID,'Null')
,IsNull(AgentTeamStatus,'Null')
,IsNull(Convert(VARCHAR(24),MPRMasterAgentID),'Null')
,CASE WHEN MLSTFNOptInFlag = 'True' THEN 1 WHEN MLSTFNOptInFlag='False' THEN 0 ELSE MLSTFNOptInFlag  END 
,IsNull(AgentFirstName,'Null')
,IsNull(AgentLastName,'Null')
,IsNull(AgentMiddleName,'Null')
,IsNull(AgentNickName,'Null')
,IsNull(AgentTitle,'Null')
,IsNull(AgentSuffix,'Null')
, CASE WHEN CoShowReportingOptoutFlg = 'True' THEN 1 WHEN CoShowReportingOptoutFlg='False' THEN 0 ELSE CoShowReportingOptoutFlg  END 
, CASE WHEN LatestFlag = 'True' THEN 1 WHEN LatestFlag='False' THEN 0 ELSE LatestFlag  END 
,IsNull(NARMembershipID,'Null')
,IsNull(SalesForceAccountID,'Null')
from edw.tblAgentDim where agentdimkey between 10443871 and 10448871 order by 1