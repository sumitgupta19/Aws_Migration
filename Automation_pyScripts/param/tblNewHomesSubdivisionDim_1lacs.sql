select
SubdivisionDimKey
,MasterSubdivisionID
,MasterBuilderID
,CASE WHEN IsDeleted = 'True' THEN 1 ELSE 0 END
,Convert(VARCHAR(24),DateIsLive,120) 
,AMSubdivisionRecordStatus
,BDXSubdivisionID
,Status
,CASE WHEN ShareWithRealtors = 'True' THEN 1 ELSE 0 END 
,CAST(PriceLow as decimal(38,6))
,CAST(PriceHigh as decimal(38,6))
,SqftLow
,SqftHigh
,MarketingChannel
,SubdivisionNumber
,SubdivisionName
,SubParentName
,SubDesc
,CASE WHEN UseDefaultLeadsEmail = 'True' THEN 1 ELSE 0 END  
,SubLeadsEmail
,SubLeadsEmailLeadsPerMessage
,SubAdrStreet1
,SubAdrStreet2
,SubAdrCounty
,SubAdrCity
,SubAdrState
,SubAdrZIP
,SubAdrCountry
,SubAdrLat
,SubAdrLong
,SubVideoTour
,DrivingDirections
,SubWebsite
,CASE WHEN BuildOnYourLot = 'True' THEN 1 ELSE 0 END  
,CommunityStyle
,PhotoCount
,SchDistName
,SchDistNameLEAID
,SchElem
,SchElemNCESID
,SchMiddle
,SchMiddleNCESID
,SchHigh
,SchHighNCESID
,ServiceHOA
,ServiceHOAMonthlyFee
,ServiceHOAName
,ServiceHOAYearlyFee
,CASE WHEN SubAmenPool = 'True' THEN 1 ELSE 0 END  
,CASE WHEN SubAmenPlayground = 'True' THEN 1 ELSE 0 END  
,CASE WHEN SubAmenGolfCourse = 'True' THEN 1 ELSE 0 END  
,CASE WHEN SubAmenTennis = 'True' THEN 1 ELSE 0 END  
,CASE WHEN SubAmenSoccer = 'True' THEN 1 ELSE 0 END  
,CASE WHEN SubAmenVolleyball = 'True' THEN 1 ELSE 0 END  
,CASE WHEN SubAmenBasketball = 'True' THEN 1 ELSE 0 END  
,CASE WHEN SubAmenBaseball = 'True' THEN 1 ELSE 0 END  
,CASE WHEN SubAmenViews = 'True' THEN 1 ELSE 0 END  
,SubAmenLake
,CASE WHEN SubAmenPond = 'True' THEN 1 ELSE 0 END  
,CASE WHEN SubAmenMarina = 'True' THEN 1 ELSE 0 END  
,CASE WHEN SubAmenBeach = 'True' THEN 1 ELSE 0 END  
,CASE WHEN SubAmenWaterfrontLots = 'True' THEN 1 ELSE 0 END  
,CASE WHEN SubAmenPark = 'True' THEN 1 ELSE 0 END  
,CASE WHEN SubAmenTrails = 'True' THEN 1 ELSE 0 END  
,CASE WHEN SubAmenGreenbelt = 'True' THEN 1 ELSE 0 END  
,CASE WHEN SubAmenClubhouse = 'True' THEN 1 ELSE 0 END  
,CASE WHEN SubAmenComCenter = 'True' THEN 1 ELSE 0 END  
,GreenProgramVal
,GreenProgramTitle
,GreenProgramRefType
,PrimaryCommunityImage
,AccreditationSeal
,Promotion
,Testimonial
,SalesOffAgt
,CASE WHEN SalesOffAdrOutOfCommunity = 'True' THEN 1 ELSE 0 END   
,SalesOffAdrStreet1
,SalesOffAdrStreet2
,SalesOffAdrCounty
,SalesOffAdrCity
,SalesOffAdrState
,SalesOffAdrZIP
,SalesOffAdrCountry
,SalesOffAdrGeoLat
,SalesOffAdrGeoLong
,SalesOffPhoneAreaCode
,SalesOffPhonePrefix
,SalesOffPhoneSuffix
,SalesOffPhoneExtension
,SalesOffEmail
,SalesOffFaxAreaCode
,SalesOffFaxPrefix
,SalesOffFaxSuffix
,SalesOffHours
,TotalTaxRate
,Taxes
,Convert(VARCHAR(24),EffectiveFrom,120) 
,Convert(VARCHAR(24),EffectiveTo,120)
,Convert(VARCHAR(24),CreatedDate,120) 
,CreatedBy
,Convert(VARCHAR(24),ModifiedDate,120) 
,Modifiedby
from edw.tblNewHomesSubdivisionDim
where SubdivisionDimKey between 1 and 10000
order by 1