select 
MemberDimKey
,MemberAccountID
,DisplayName
,FirstName
,LastName
,Gender
,EmailAddress
,MemberType
,PostalCode
,RegistrationSourceChannel
,RegistrationStatus
,CancellationReason
,Convert(VARCHAR(24),ProfileCreateDateMST) 
,Convert(VARCHAR(24),ProfileUpdateDateMST) 
,FacebookID
,HasFacebookIDFlag 
,FacebookStatus
,ReferredBy
,InAreaSince
,InCurrentHomeSince
,Convert(VARCHAR(24),iphoneUserStartDate) 
,Convert(VARCHAR(24),iphoneLastLoginDate) 
,Convert(VARCHAR(24),UserTypeConversionDate) 
,Convert(VARCHAR(24),EffectiveFrom) 
,Convert(VARCHAR(24),EffectiveTo) 
,Convert(VARCHAR(24),CreatedDate) 
,CreatedBy
,Convert(VARCHAR(24),ModifiedDate) 
,Modifiedby
,SourceApplicationName
,Interests0
,Interests1
,Interests2
,Interests3
,Interests4
,Interests5
,Interests6
,Convert(VARCHAR(24),MemberProfileMoveDate)  
,CommunicationPreferencesForm0
,CommunicationPreferencesDeliveriesMethod00
,CommunicationPreferencesDeliveriesFrequency00
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesCreatedAt00)  
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesUpdatedAt00)  
,CommunicationPreferencesDeliveriesMethod01
,CommunicationPreferencesDeliveriesFrequency01
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesCreatedAt01) 
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesUpdatedAt01)  
,CommunicationPreferencesDeliveriesMethod02
,CommunicationPreferencesDeliveriesFrequency02
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesCreatedAt02)  
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesUpdatedAt02)  
,CommunicationPreferencesForm1
,CommunicationPreferencesDeliveriesMethod10
,CommunicationPreferencesDeliveriesFrequency10
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesCreatedAt10)  
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesUpdatedAt10)  
,CommunicationPreferencesDeliveriesMethod11
,CommunicationPreferencesDeliveriesFrequency11
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesCreatedAt11)  
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesUpdatedAt11)  
,CommunicationPreferencesDeliveriesMethod12
,CommunicationPreferencesDeliveriesFrequency12
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesCreatedAt12)
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesUpdatedAt12)
,CommunicationPreferencesForm2
,CommunicationPreferencesDeliveriesMethod20
,CommunicationPreferencesDeliveriesFrequency20
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesCreatedAt20)
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesUpdatedAt20)
,CommunicationPreferencesDeliveriesMethod21
,CommunicationPreferencesDeliveriesFrequency21
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesCreatedAt21)
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesUpdatedAt21)
,CommunicationPreferencesDeliveriesMethod22
,CommunicationPreferencesDeliveriesFrequency22
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesCreatedAt22)
,Convert(VARCHAR(24),CommunicationPreferencesDeliveriesUpdatedAt22)
from RegistrationDM.tblMemberDim
where MemberDimKey between 1 and 8916