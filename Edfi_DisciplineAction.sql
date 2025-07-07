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
TEMP_INCIDENT_ACTION AS (
    SELECT * FROM $source_lakehouse_name.dbo.PS_INCIDENT_ACTION
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
    CONCAT('ps_', SHA2(CAST(COALESCE(iac.DCID, '') AS STRING), 256)) AS id,

    ils_resp.state_detail_report_code AS responsibilitySchoolId,
    ils_assign.state_detail_report_code AS assignmentSchoolId,

    s.state_studentnumber AS studentUniqueId,

    LPAD(iac.action_number, 3, '0') AS disciplineActionIdentifier,
    CAST(iac.action_plan_begin_dt AS DATE) AS disciplineDate,

    ilc_duration.state_aggregate_rpt_code AS disciplineActionLengthDifferenceReasonDescriptor,

    ils_discipline.state_detail_report_code AS disciplineDescriptor,
    txi.State_Incident_Id AS incidentIdentifier,

    inc.school_number AS schoolId,

    ils_behavior.state_detail_report_code AS behaviorDescriptor,

    iac.duration_assigned AS tx_officialLengthOfDisciplinaryAssignment,
    iac.duration_actual AS tx_actualLengthOfDisciplinaryAssignment,

    ils_inconsistent.state_detail_report_code AS tx_inconsistentCodeOfConduct,
    ils_nonmembership.state_detail_report_code AS tx_nonMembershipDisciplineRestraintIndicator,

    sch.school_name AS display_responsibilitySchoolName,
    s.student_number AS display_studentNumber,
    s.lastfirst AS display_studentName,

    GREATEST(
        COALESCE(inc.rundate, '1900-01-01'),
        COALESCE(ind.rundate, '1900-01-01'),
        COALESCE(iac.rundate, '1900-01-01')
    ) AS _META_RUNDATE,

    inc._META_SCHOOL_YEAR AS _META_SCHOOL_YEAR,
    inc._META_DISTRICT_ID AS _META_DISTRICT_ID

FROM TEMP_INCIDENT inc

LEFT JOIN TEMP_INCIDENT_DETAIL ind
    ON inc.incident_id = ind.incident_id

LEFT JOIN TEMP_INCIDENT_ACTION iac
    ON inc.incident_id = iac.incident_id

LEFT JOIN TEMP_INCIDENT_PERSON_ROLE ipr
    ON inc.incident_id = ipr.incident_id

LEFT JOIN TEMP_STUDENTS s
    ON ipr.studentid = s.student_number

LEFT JOIN TEMP_SCHOOLS sch
    ON sch.school_number = inc.school_number

LEFT JOIN TEMP_S_TX_INC_X txi
    ON inc.incident_id = txi.incident_id

-- Responsibility SchoolId
LEFT JOIN TEMP_INCIDENT_LU_CODE ilc_resp
    ON ilc_resp.lu_code_id = ind.lu_code_id
   AND ilc_resp.code_type = 'actionattribute'
   AND ilc_resp.state_aggregate_rpt_code = 'CampusOfResp'
LEFT JOIN TEMP_INCIDENT_LU_SUB_CODE ils_resp
    ON ils_resp.lu_code_id = ilc_resp.lu_code_id

-- Assignment SchoolId
LEFT JOIN TEMP_INCIDENT_LU_CODE ilc_assign
    ON ilc_assign.lu_code_id = ind.lu_code_id
   AND ilc_assign.code_type = 'actionattribute'
   AND ilc_assign.state_aggregate_rpt_code = 'CampusOfAssignment'
LEFT JOIN TEMP_INCIDENT_LU_SUB_CODE ils_assign
    ON ils_assign.lu_code_id = ilc_assign.lu_code_id

-- Discipline Descriptor
LEFT JOIN TEMP_INCIDENT_LU_CODE ilc_discipline
    ON ilc_discipline.lu_code_id = ind.lu_code_id
   AND ilc_discipline.code_type = 'actioncode'
LEFT JOIN TEMP_INCIDENT_LU_SUB_CODE ils_discipline
    ON ils_discipline.lu_code_id = ilc_discipline.lu_code_id

-- Behavior Descriptor
LEFT JOIN TEMP_INCIDENT_LU_CODE ilc_behavior
    ON ilc_behavior.lu_code_id = ind.lu_code_id
   AND ilc_behavior.code_type = 'behaviorcode'
LEFT JOIN TEMP_INCIDENT_LU_SUB_CODE ils_behavior
    ON ils_behavior.lu_code_id = ilc_behavior.lu_code_id

-- Duration Reason Descriptor
LEFT JOIN TEMP_INCIDENT_LU_CODE ilc_duration
    ON ilc_duration.lu_code_id = ind.lu_code_id
   AND ilc_duration.code_type = 'durationcode'

-- Inconsistent Code of Conduct
LEFT JOIN TEMP_INCIDENT_LU_CODE ilc_inconsistent
    ON ilc_inconsistent.lu_code_id = ind.lu_code_id
   AND ilc_inconsistent.code_type = 'actionattribute'
   AND ilc_inconsistent.state_aggregate_rpt_code = 'CodeofConduct'
LEFT JOIN TEMP_INCIDENT_LU_SUB_CODE ils_inconsistent
    ON ils_inconsistent.lu_code_id = ilc_inconsistent.lu_code_id

-- Non-Membership Discipline Restraint Indicator
LEFT JOIN TEMP_INCIDENT_LU_CODE ilc_nonmembership
    ON ilc_nonmembership.lu_code_id = ind.lu_code_id
   AND ilc_nonmembership.code_type = 'actionattribute'
   AND ilc_nonmembership.state_aggregate_rpt_code = 'NonMembershipInd'
LEFT JOIN TEMP_INCIDENT_LU_SUB_CODE ils_nonmembership
    ON ils_nonmembership.lu_code_id = ilc_nonmembership.lu_code_id
;
