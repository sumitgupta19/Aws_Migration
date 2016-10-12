select
SubdivisionDimKey
,MasterSubdivisionID
,MasterBuilderID
,IsDeleted
,Convert(VARCHAR(24),DateIsLive) 
,AMSubdivisionRecordStatus
,BDXSubdivisionID
,Status
,ShareWithRealtors
,PriceLow
,PriceHigh
,SqftLow
,SqftHigh
,MarketingChannel
,SubdivisionNumber
,SubdivisionName
,SubParentName
,SubDesc
,UseDefaultLeadsEmail
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
,BuildOnYourLot
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
,SubAmenPool
,SubAmenPlayground
,SubAmenGolfCourse
,SubAmenTennis
,SubAmenSoccer
,SubAmenVolleyball
,SubAmenBasketball
,SubAmenBaseball
,SubAmenViews
,SubAmenLake
,SubAmenPond
,SubAmenMarina
,SubAmenBeach
,SubAmenWaterfrontLots
,SubAmenPark
,SubAmenTrails
,SubAmenGreenbelt
,SubAmenClubhouse
,SubAmenComCenter
,GreenProgramVal
,GreenProgramTitle
,GreenProgramRefType
,PrimaryCommunityImage
,AccreditationSeal
,Promotion
,Testimonial
,SalesOffAgt
,SalesOffAdrOutOfCommunity
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
,Convert(VARCHAR(24),EffectiveFrom) 
,Convert(VARCHAR(24),EffectiveTo)
,Convert(VARCHAR(24),CreatedDate) 
,CreatedBy
,Convert(VARCHAR(24),ModifiedDate) 
,Modifiedby
from edw.tblNewHomesSubdivisionDim
where SubdivisionDimKey between 5001 and 10000
order by 1 