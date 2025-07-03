WITH TEMP_INCIDENT AS (
    SELECT * FROM $source_lakehouse_name.dbo.PS_INCIDENT
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_INCIDENT_DETAIL AS (
    SELECT * FROM $source_lakehouse_name.dbo.PS_INCIDENT_DETAIL
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_INCIDENT_LU_CODE AS (
    SELECT * FROM $source_lakehouse_name.dbo.PS_INCIDENT_LU_CODE
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_INCIDENT_LU_SUB_CODE AS (
    SELECT * FROM $source_lakehouse_name.dbo.PS_INCIDENT_LU_SUB_CODE
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_INCIDENT_PERSON_ROLE AS (
    SELECT * FROM $source_lakehouse_name.dbo.PS_INCIDENT_PERSON_ROLE
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_INCIDENT_PERSON_DETAIL AS (
    SELECT * FROM $source_lakehouse_name.dbo.PS_INCIDENT_PERSON_DETAIL
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),
