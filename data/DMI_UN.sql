/****** Script for HMTASS ENTITY AND PERSON from SSMS  ******/
SELECT a.[EntityId]*1 as "PEID"
	, u."WL_UN_Permanent_Reference_No"
	, u.code as "Ref No. Provenance"
	, coalesce(tt."WL Name"+tt.Name, tt."WL Name", tt.Name) as "WL_Name"
	, tt."Name Type" AS "WL_Name_Type"
	, tt.code as "Name Provenance"
	, tt.EntityNameId
	, tt.Surname AS "WL_Surname"
	, tt.ParentNameType
	, k."WL_DoB"
	, k."WL_DoB_ExternalComment"
	, k.code "DoB Provenance"
	, k.EntityDateId
	, qq.PoB AS "WL_PoB"
	, qq.PersonPlaceOfBirthId
	, q.Name AS "WL_Country"
	, q.Address
	, q.CityZip
	, q.StateProvince
	, q.EntityAddressId
	, n."WL_ID_No"
	, n."WL ID Type"
	, n."WL ID Notes"
	, n.code as "ID Provenance"
	, n.EntityIdentificationId
	, sl.Name as "List Name"
	, sl.code as "List Provenance"
	, nationality.Name AS "WL_Nationality"
	, nationality.EntityCountryId AS "EntityNationalityId"
	, residency.Name AS "WL_Residency"
	, residency.EntityCountryId AS "EntityResidencyId"

FROM [SDM].[RnC].[Entity] a
	INNER JOIN [SDM].[Look].[EntityStatus] f ON a.EntityStatusId=f.EntityStatusId

	/****** All Names And Types and Subtypes ******/
	LEFT JOIN ( Select [EntityId], EntityNameId, "Name Type",
		case when LEN(Prefix) > 0
		then LTRIM(RTRIM(Prefix)) + ' '
		else ''
		end +
	case
		 when LEN(FirstName) > 0
		 then LTRIM(RTRIM(FirstName))
		 else ''
		 end +
	case
		 when LEN(FirstName) > 0 and LEN(Surname) > 0
		 then ' '
		 else ''
		 end +
	case
		 when LEN(MiddleName) > 0
		 then LTRIM(RTRIM(MiddleName)) + ' '
		 else ''
		 end +
	case
		 when LEN(Surname) > 0
		 then LTRIM(RTRIM(Surname))
		 else ''
		 end +
	case
		 when LEN(Suffix) > 0
		 then ' ' + LTRIM(RTRIM(Suffix))
		 else ''
		 end as "WL Name"
	, Name
	, t.ParentNameType
	, t.Surname
	, t.code

	FROM
		(SELECT a.[EntityId], g.Name as "Name Type", a.Name, [FirstName], [MiddleName], [Surname], [Suffix], "Prefix", a.EntityNameId, parent.NameType AS ParentNameType, c.code

		FROM [SDM].[RnC].[EntityName] a
			LEFT JOIN [SDM].[RnC].[Provenance] b ON a.EntityId=b.EntityId AND a.EntityNameId=b.KeyId
			INNER JOIN [SDM].[Look].[ProvenanceType] c ON b.ProvenanceTypeId=c.ProvenanceTypeId
			INNER JOIN [SDM].[Look].[Column] d ON b.ColumnId=d.ColumnId
			INNER JOIN [SDM].[Look].[Table] e ON d.TableId=e.TableId
			INNER JOIN [SDM].[Look].[EntityTypeNameType] f ON a.EntityTypeNameTypeId=f.EntityTypeNameTypeId
			INNER JOIN [SDM].[Look].[NameType] g ON f.NameTypeId=g.NameTypeId
			LEFT JOIN (
				SELECT
					a.[EntityId]
					, EntitynameId
					, c.[Name] AS NameType
				FROM SDM.[RnC].[EntityName] a
				INNER JOIN SDM.[Look].[EntityTypeNameType] b ON a.[EntityTypeNameTypeId]=b.[EntityTypeNameTypeId]
				INNER JOIN SDM.Look.NameType c ON b.[NameTypeId]=c.[NameTypeId]
				WHERE a.IsDeleted=0
			) AS parent ON parent.EntitynameId=a.ParentEntitynameId AND a.[EntityId]=parent.[EntityId]
		Where g.Name IN ('Primary Name','Also Known As','Maiden Name','Formerly Known As','Spelling Variation', 'Low Quality AKA', 'SSN', 'OSN', 'Expanded Language Variation', 'Reclassified AKA')
		AND e.Name IN ('EntityNameProvenance') AND c.Code IN ('UNCON', 'UNTAL', 'UNOT') AND a.IsDeleted=0 AND b.IsDeleted=0 AND b.IsActive=1
		) t
	) tt ON a.EntityId=tt.EntityId

	/****** DOB  ******/
	left join(SELECT a.EntityId,
		case
		when LEN("Day") > 0 AND "Day" <> 0
		then CAST("Day" AS nvarchar) + ' '
		else ''
	end +
	case
		when LEN("Month") > 0
		then "Month" + ' '
		else ''
	end +
	case
		when LEN("Year") > 0
		then CAST("Year" AS nvarchar)
		else ''
	end as "WL_DoB"
	, REPLACE(REPLACE(Note, CHAR(13), ''), CHAR(10), ' ') as "WL_DoB_ExternalComment"
	, a.EntityDateId
	, c.code

	FROM [SDM].[RnC].[EntityDate] a
		LEFT JOIN [SDM].[RnC].[Provenance] b ON a.EntityId=b.EntityId AND a.EntityDateId=b.KeyId
		INNER JOIN [SDM].[Look].[ProvenanceType] c ON b.ProvenanceTypeId=c.ProvenanceTypeId
		INNER JOIN [SDM].[Look].[Column] d ON b.ColumnId=d.ColumnId
		INNER JOIN [SDM].[Look].[Table] e ON d.TableId=e.TableId
		INNER JOIN [SDM].[Look].[DateType] f on a.DateTypeId=f.DateTypeId
	WHERE f.Name='Date of Birth' AND e.Name IN ('EntityDateProvenance') AND c.Code IN ('UNCON', 'UNTAL', 'UNOT') AND a.IsDeleted=0 AND b.IsDeleted=0 AND b.IsActive=1
	) k on k.EntityId=a.EntityId

	/****** Addresses  ******/
	LEFT JOIN (SELECT a.EntityAddressId, a.EntityId, a.Address
	, case
		 when LEN(a.ZipCode) > 0
		 then a.AddressCity + ' ' + a.ZipCode
		 else a.AddressCity
		 end as CityZip
	, f.Name
	, a.StateProvince
	, c.code

	FROM [SDM].[RnC].[EntityAddress] a
		LEFT JOIN [SDM].[RnC].[Provenance] b ON a.EntityId=b.EntityId AND a.EntityAddressId=b.KeyId
		INNER JOIN [SDM].[Look].[ProvenanceType] c ON b.ProvenanceTypeId=c.ProvenanceTypeId
		INNER JOIN [SDM].[Look].[Column] d ON b.ColumnId=d.ColumnId
		INNER JOIN [SDM].[Look].[Table] e ON d.TableId=e.TableId
		LEFT JOIN [SDM].[Look].[Country] f ON a.CountryId=f.CountryId
	WHERE e.Name IN ('EntityAddressProvenance') AND c.Code IN ('UNCON', 'UNTAL', 'UNOT') AND a.IsDeleted=0 AND b.IsDeleted=0 AND b.IsActive=1
	) q ON a.EntityId=q.EntityId

	/****** ID Types ******/
	/****** UN Permanent Reference No. ******/
	LEFT JOIN (SELECT a.EntityId
	, "Number" AS "WL_UN_Permanent_Reference_No"
	, c.code

	FROM [SDM].[RnC].[EntityIdentification] a
		LEFT JOIN [SDM].[RnC].[Provenance] b ON a.EntityId=b.EntityId AND a.EntityIdentificationId=b.KeyId
		INNER JOIN [SDM].[Look].[ProvenanceType] c ON b.ProvenanceTypeId=c.ProvenanceTypeId
		INNER JOIN [SDM].[Look].[Column] d ON b.ColumnId=d.ColumnId
		INNER JOIN [SDM].[Look].[Table] e ON d.TableId=e.TableId
		LEFT JOIN [SDM].[Look].[IdentificationType] f ON a.IdentificationTypeId=f.IdentificationTypeId
	WHERE f.Name IN ('UN Permanent Reference No.') AND e.Name IN ('EntityIdentificationProvenance') AND c.Code IN ('UNCON', 'UNTAL', 'UNOT') AND a.IsDeleted=0 AND b.IsDeleted=0 AND b.IsActive=1
	) u ON a.EntityId=u.EntityId

	/****** ID No. ******/
	LEFT JOIN (SELECT a.EntityId
	, f.Name AS "WL ID Type"
	, "Number" AS "WL_ID_No"
	, REPLACE(REPLACE(Note, CHAR(13), ''), CHAR(10), ' ') AS "WL ID Notes"
	, a.EntityIdentificationId
	, c.code

	FROM [SDM].[RnC].[EntityIdentification] a
		LEFT JOIN [SDM].[RnC].[Provenance] b ON a.EntityId=b.EntityId AND a.EntityIdentificationId=b.KeyId
		INNER JOIN [SDM].[Look].[ProvenanceType] c ON b.ProvenanceTypeId=c.ProvenanceTypeId
		INNER JOIN [SDM].[Look].[Column] d ON b.ColumnId=d.ColumnId
		INNER JOIN [SDM].[Look].[Table] e ON d.TableId=e.TableId
		LEFT JOIN [SDM].[Look].[IdentificationType] f ON a.IdentificationTypeId=f.IdentificationTypeId
	WHERE f.Name NOT IN ('UN Permanent Reference No.') AND e.Name IN ('EntityIdentificationProvenance') AND c.Code IN ('UNCON', 'UNTAL', 'UNOT') AND a.IsDeleted=0 AND b.IsDeleted=0 AND b.IsActive=1
	) n ON a.EntityId=n.EntityId

	/****** POB ******/
	LEFT JOIN (SELECT a.[EntityId]
	, case
		when LEN(City) > 0
		then City + ' '
		else ''
	end +
	case
		when LEN(StateProvince) > 0
		then StateProvince + ' '
		else ''
	end +
	case
		when LEN(County) > 0
		then County + ' '
		else ''
	end +
	case
		when LEN(f.name) > 0
		then f.name
		else ''
	end +
	case
		when LEN(ZipCode) > 0
		then ' ' + ZipCode
		else ''
	end as "PoB"
	, a.PersonPlaceOfBirthId
	, c.code

	FROM [SDM].[RnC].[PersonPlaceOfBirth] a
		LEFT JOIN [SDM].[RnC].[Provenance] b ON a.EntityId=b.EntityId AND a.PersonPlaceOfBirthId=b.KeyId
		INNER JOIN [SDM].[Look].[ProvenanceType] c ON b.ProvenanceTypeId=c.ProvenanceTypeId
		INNER JOIN [SDM].[Look].[Column] d ON b.ColumnId=d.ColumnId
		INNER JOIN [SDM].[Look].[Table] e ON d.TableId=e.TableId
		LEFT JOIN [SDM].[Look].[Country] f ON a.CountryId=f.CountryId
	Where e.Name IN ('PersonPlaceOfBirthProvenance') AND c.Code IN ('UNCON', 'UNTAL', 'UNOT') AND a.IsDeleted=0 AND b.IsDeleted=0 AND b.IsActive=1
	) qq ON a.EntityId=qq.EntityId

	/****** Sanction List ******/
	LEFT JOIN (SELECT a.EntityId,
		c.Code
	, case
		when LEN(a.ToDay) > 0 AND a.ToDay <> 0
		then CAST(a.ToDay AS nvarchar) + ' '
		else ''
	end +
	case
		when LEN(a.ToMonth) > 0
		then a.ToMonth + ' '
		else ''
	end +
	case
		when LEN(a.ToYear) > 0
		then CAST(a.ToYear AS nvarchar)
		else ''
	end as ToDate
	, g.Name

	FROM [SDM].[RnC].[EntityListReference] a
		LEFT JOIN [SDM].[RnC].[Provenance] b ON a.EntityId=b.EntityId AND a.EntityListReferenceId=b.KeyId
		INNER JOIN [SDM].[Look].[ProvenanceType] c ON b.ProvenanceTypeId=c.ProvenanceTypeId
		INNER JOIN [SDM].[Look].[Column] d ON b.ColumnId=d.ColumnId
		INNER JOIN [SDM].[Look].[Table] e ON d.TableId=e.TableId
		LEFT JOIN [SDM].[Look].[ListReferenceContentType] f ON a.ListReferenceContentTypeId=f.ListReferenceContentTypeId
		INNER JOIN [SDM].[Look].[ListReference] g ON f.ListReferenceId=g.ListReferenceId
	WHERE e.Name IN ('EntityListReferenceProvenance') AND c.Code IN ('UNCON', 'UNTAL', 'UNOT') AND a.IsDeleted=0 AND b.IsDeleted=0 AND b.IsActive=1
	) sl ON a.EntityId=sl.EntityId


	/****** Nationality ******/
	LEFT JOIN (
	SELECT DISTINCT
		EntityCountry.[EntityId]
	  , country_type_name.Name as CountryTypeName
	  , country.Name
	  , EntityCountry.EntityCountryId
	FROM [SDM].[RnC].[EntityCountry] EntityCountry
		INNER JOIN [SDM].[RnC].[Provenance] provenance_type on EntityCountry.EntityId = provenance_type.EntityId and EntityCountry.EntityCountryId = provenance_type.KeyId
		INNER JOIN SDM.Look.Country country on EntityCountry.CountryId = country.CountryId
		INNER JOIN [SDM].[RnC].[Entity] active_ofac_id on active_ofac_id.EntityId = EntityCountry.EntityId
		INNER JOIN [SDM].[Look].[CountryType] country_type_name on EntityCountry.CountryTypeId = country_type_name.CountryTypeId
		INNER JOIN [SDM].[Look].[ProvenanceType] provenance_name ON provenance_type.ProvenanceTypeId=provenance_name.ProvenanceTypeId
	WHERE (EntityCountry.CountryTypeId = 1)
		AND provenance_name.Code IN ('UNCON', 'UNTAL', 'UNOT')
		AND EntityCountry.IsDeleted = 0
		AND provenance_type.IsDeleted = 0
		AND provenance_type.IsActive = 1
		AND active_ofac_id.EntityStatusId = 1
) nationality ON a.EntityId = nationality.EntityId

	/****** Residency ******/
	LEFT JOIN (
	SELECT DISTINCT
		EntityCountry.[EntityId]
	  , country_type_name.Name as CountryTypeName
	  , country.Name
	  , EntityCountry.EntityCountryId
	FROM [SDM].[RnC].[EntityCountry] EntityCountry
		INNER JOIN [SDM].[RnC].[Provenance] provenance_type on EntityCountry.EntityId = provenance_type.EntityId and EntityCountry.EntityCountryId = provenance_type.KeyId
		INNER JOIN SDM.Look.Country country on EntityCountry.CountryId = country.CountryId
		INNER JOIN [SDM].[RnC].[Entity] active_ofac_id on active_ofac_id.EntityId = EntityCountry.EntityId
		INNER JOIN [SDM].[Look].[CountryType] country_type_name on EntityCountry.CountryTypeId = country_type_name.CountryTypeId
		INNER JOIN [SDM].[Look].[ProvenanceType] provenance_name ON provenance_type.ProvenanceTypeId=provenance_name.ProvenanceTypeId
	WHERE (EntityCountry.CountryTypeId = 2 OR EntityCountry.CountryTypeId = 3)
		AND provenance_name.Code IN ('UNCON', 'UNTAL', 'UNOT')
		AND EntityCountry.IsDeleted = 0
		AND provenance_type.IsDeleted = 0
		AND provenance_type.IsActive = 1
		AND active_ofac_id.EntityStatusId = 1
) residency ON a.EntityId = residency.EntityId

WHERE sl.Code IN ('UNCON', 'UNTAL', 'UNOT') AND f."Description"='Live' AND datalength(sl.ToDate)=0
Order By a.EntityId