WITH TEMP_S_TX_STU_SCHOOL_ENR_C AS (
    SELECT *
    FROM $source_lakehouse_name.dbo.PS_S_TX_STU_SCHOOL_ENR_C
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}' 
),

TEMP_STUDENTS AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_STUDENTS
    WHERE DCID RLIKE '^[0-9]+$'
    AND _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_S_STU_X AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_S_STU_X
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}' 
),

SELECT 
    CONCAT('ps_', SHA2(CAST(COALESCE(stu.DCID, '') AS STRING), 256))            AS id,
    enr.SCHOOLID                                                                AS schoolId,
    stu.State_StudentNumber                                                     AS studentUniqueId,  
    enr.ENTRYDATE                                                               AS entryDate,
    enr.EXITDATE                                                                AS exitWithdrawDate,
    enr.EXITCODE                                                                AS exitWithdrawTypeDesciptor,
    enr.ENTRYCODE                                                               AS entryTypeDescriptor,
    enr.GRADELEVEL                                                              AS entryGradeLevelDescriptor,
    enr.TRACK                                                                   AS calendarCode,
    enr.ADA_ELIGIBILITY                                                         AS tx_adaEligibilityDescriptor,
    enr.STUDENT_ATTRIBUTION                                                     AS tx_studentAttributionDescriptor,
    enr.CAMPUS_RESIDENCE                                                        AS tx_campusIdOfResidence,
    enr.CAMPUS_ACCOUNTABILITY                                                   AS tx_campusIdOfAccountability,
    null                                                                        AS tx_enrollmentTrackingVerificationDescriptor,
    GREATEST(stu.rundate, scf.rundate, txt.rundate)                             AS _META_RUNDATE,
    stu._META_SCHOOL_YEAR                                                       AS _META_SCHOOL_YEAR,
    stu.student_number                                                          AS student_number,
    stu.lastfirst                                                               AS Student_name

FROM TEMP_S_TX_STU_SCHOOL_ENR_C enr
LEFT JOIN TEMP_STUDENTS stu ON enr.StudentsDCID = stu.DCID
LEFT JOIN TEMP_S_STU_X stux ON enr.StudentsDCID = stux.StudentsDCID
