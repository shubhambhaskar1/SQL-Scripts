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

TEMP_INCIDENT_ACTION AS (
    SELECT * FROM $source_lakehouse_name.dbo.PS_INCIDENT_ACTION
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_INCIDENT_PERSON_ACTION AS (
    SELECT * FROM $source_lakehouse_name.dbo.PS_INCIDENT_PERSON_ACTION
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_STUDENTS AS (
    SELECT * FROM $source_lakehouse_name.dbo.PS_STUDENTS
    WHERE DCID RLIKE '^[0-9]+$'
    AND _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_S_TX_INC_X AS (
    SELECT * FROM $source_lakehouse_name.dbo.PS_S_TX_INC_X
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
)



SELECT
    ipr.studentid                                            AS studentUniqueId,
    inc.school_number                                        AS schoolId,
    S_TX_INC_X.State_Incident_Id                             AS incidentIdentifier,
    case 
      when ilc.code_type = 'behaviorcode'   
      then ils.state_detail_report_code else null 
    end                                                      AS behaviorDescriptor, -- Need to apply where condition for the 
    s.student_number                                         AS studentNumber

FROM TEMP_INCIDENT inc

LEFT JOIN TEMP_INCIDENT_PERSON_ROLE ipr
    ON inc.incident_id = ipr.incident_id

LEFT JOIN TEMP_INCIDENT_ACTION iac
    ON inc.incident_id = iac.incident_id

LEFT JOIN TEMP_INCIDENT_DETAIL ind
    ON inc.incident_id = ind.incident_id

LEFT JOIN TEMP_INCIDENT_LU_CODE ilc
    ON ilc.lu_code_id = ind.lu_code_id

LEFT JOIN TEMP_INCIDENT_LU_SUB_CODE ils
    ON ils.lu_code_id = ilc.lu_code_id

LEFT JOIN TEMP_S_TX_INC_X S_TX_INC_X
    ON inc.incident_id = S_TX_INC_X.incident_id

LEFT JOIN TEMP_STUDENTS stu
    ON ipr.studentid = stu.student_number
