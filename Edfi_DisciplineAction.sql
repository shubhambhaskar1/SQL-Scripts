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

TEMP_INCIDENT_ACTION_ATTRIBUTE AS (
    SELECT * FROM $source_lakehouse_name.dbo.PS_INCIDENT_ACTION_ATTRIBUTE
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_STUDENTS AS (
    SELECT * FROM $source_lakehouse_name.dbo.PS_STUDENTS
    WHERE DCID RLIKE '^[0-9]+$'
    AND _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_SCHOOLS AS (
    SELECT * FROM $source_lakehouse_name.dbo.PS_SCHOOLS
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_S_TX_INC_X AS (
    SELECT * FROM $source_lakehouse_name.dbo.PS_S_TX_INC_X
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
)

SELECT
    CONCAT('ps_', SHA2(CAST(COALESCE(iac.DCID, '') AS STRING), 256))                  AS id,                      -- Unique Ed-Fi ID
    ils.state_detail_report_code                                                      AS responsibilitySchoolId,    -- E1037
    ils.state_detail_report_code                                                      AS assignmentSchoolId,        -- E1003
    ipr.studentid                                                                     AS studentUniqueId,           -- E1523
    NULL                                                                              AS disciplineActionIdentifier, -- E1004 (placeholder; confirm column)
    iac.action_plan_begin_dt                                                          AS disciplineDate,            -- E1036
    ilc.state_aggregate_rpt_code                                                      AS disciplineActionLengthDifferenceReasonDescriptor, -- E1009
    ils.state_detail_report_code                                                      AS disciplineDescriptor,      -- E1005
    S_TX_INC_X.State_Incident_Id                                                      AS incidentIdentifier,        -- E1016
    inc.school_number                                                                 AS schoolId,                  -- E0782
    ils.state_detail_report_code                                                      AS behaviorDescriptor,        -- E1006
    iac.duration_assigned                                                             AS tx_officialLengthOfDisciplinaryAssignment, -- E1007
    iac.duration_actual                                                               AS tx_actualLengthOfDisciplinaryAssignment,   -- E1008
    ils.state_detail_report_code                                                      AS tx_inconsistentCodeOfConduct, -- E1656
    ils.state_detail_report_code                                                      AS tx_nonMembershipDisciplineRestraintIndicator, -- E1725
    sch.school_name                                                                   AS display_responsibilitySchoolName,   -- Display only
    s.student_number                                                                  AS display_studentNumber,              -- Display only
    s.lastfirst                                                                       AS display_studentName,                -- Display only
    GREATEST(inc.rundate, ind.rundate, iac.rundate)                                   AS _META_RUNDATE,
    inc._META_SCHOOL_YEAR                                                             AS _META_SCHOOL_YEAR,
    inc._META_DISTRICT_ID                                                             AS _META_DISTRICT_ID

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

LEFT JOIN TEMP_STUDENTS s
    ON ipr.studentid = s.student_number

LEFT JOIN TEMP_SCHOOLS sch
    ON sch.school_number = inc.school_number
