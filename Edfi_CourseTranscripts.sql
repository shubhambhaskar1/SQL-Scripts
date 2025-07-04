
TEMP_STUDENTS AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_STUDENTS
    WHERE DCID RLIKE '^[0-9]+$'
    AND _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_STOREDGRADES AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_STOREDGRADES
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_CC AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_CC
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_SECTIONS AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_SECTIONS
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_COURSES AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_COURSES
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_TERMS AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_TERMS
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_GRADESCALEITEM AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_GRADESCALEITEM
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_COURSESCOREFIELDS AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_COURSESCOREFIELDS
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_S_TX_TRM_C AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_S_TX_TRM_C
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_S_TX_GSI_X AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_S_TX_GSI_X
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_S_TX_SERVICE_ID_S AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_S_TX_SERVICE_ID_S
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_S_TX_SGR_X AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_S_TX_SGR_X
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_S_TX_SEC_X AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_S_TX_SEC_X
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_S_TX_CRS_X AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_S_TX_CRS_X
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
)

SELECT
    sg.studentid                                                                                                      AS studentUniqueId,
    sg.schoolid                                                                                                       AS educationOrganizationId,
    sg.storecode                                                                                                      AS termDescriptor,
    (sg.termid / 100) + 1991                                                                                          AS schoolYear,
    -- courseReference: courseCode with priority logic
    CASE
        WHEN sgx.alt_course_number IS NOT NULL THEN sgx.alt_course_number
        WHEN secx.alt_course_number IS NOT NULL THEN secx.alt_course_number
        WHEN ccf.alt_course_number IS NOT NULL THEN ccf.alt_course_number
        WHEN LENGTH(TRIM(crs.course_number)) = 8 THEN crs.course_number
        ELSE NULL
    END                                                                                                               AS courseCode,
    -- courseAttemptResultDescriptor
    CASE
        WHEN sgx.E0949_PF_Calc IS NOT NULL THEN sgx.E0949_PF_Calc
        WHEN sg.GPA_Custom2 IS NOT NULL THEN sg.GPA_Custom2
        WHEN svc.elig_for_hs = 1 THEN
            CASE
                WHEN sg.earnedcrhrs = sg.potentialcrhrs AND sg.potentialcrhrs > 0 THEN '01'
                WHEN sg.percent >= 70 THEN '01'
                WHEN TRIM(sg.grade) = 'I' THEN '09'
                WHEN sg.percent < 70 AND (sg.potentialcrhrs > 0 OR sg.percent > 0) THEN '02'
                WHEN xtgsi.passing_grade = 1 THEN '01'
                WHEN xtgsi.passing_grade = 0 OR xtgsi.passing_grade IS NULL THEN '02'
                ELSE '09'
            END
        ELSE
            CASE
                WHEN sg.percent >= 70 THEN '13'
                WHEN sg.percent < 70 AND sg.percent > 0 THEN '14'
                WHEN xtgsi.passing_grade = 1 THEN '13'
                WHEN xtgsi.passing_grade = 0 OR xtgsi.passing_grade IS NULL THEN '14'
                ELSE '13'
            END
    END                                                                                                                AS courseAttemptResultDescriptor,
    sg.grade_level                                                                                                     AS whenTakenGradeLevel,
    sg.grade                                                                                                           AS finalLetterGradeEarned,
    sg.percent                                                                                                         AS finalNumericGradeEarned,
    sg.potentialcrhrs                                                                                                  AS attemptedCredits,
    sg.earnedcrhrs                                                                                                     AS earnedCredits,
    crs.course_name                                                                                                    AS courseTitle,
    crs.course_number                                                                                                  AS alternativeCourseCode,
    -- TX Extensions
    COALESCE(sgx.College_Credit_Hours, secx.E1081_College_Credit, crsx.E1081)                                          AS tx_collegeCreditHours,
    COALESCE(sgx.Dual_Credit, secx.E1011_Dual_Credit, crsx.E1011_Dual_Credit)                                          AS tx_dualCreditIndicator,
    COALESCE(sgx.ATC_Indicator_Code, secx.E1058_ATC_Indicator, crsx.E1058_ATC_Indicator)                               AS tx_atcIndicator
FROM TEMP_STOREDGRADES sg
LEFT JOIN TEMP_S_TX_SGR_X sgx ON sg.id = sgx.storedgradeid
LEFT JOIN TEMP_STUDENTS s ON sg.studentid = s.dcid
LEFT JOIN TEMP_CC cc ON sg.ccid = cc.id
LEFT JOIN TEMP_SECTIONS sec ON cc.sectionid = sec.id
LEFT JOIN TEMP_S_TX_SEC_X secx ON sec.id = secx.sectionid
LEFT JOIN TEMP_COURSES crs ON sg.course_number = crs.course_number
LEFT JOIN TEMP_S_TX_CRS_X crsx ON crs.course_number = crsx.course_number
LEFT JOIN TEMP_TERMS trm ON sg.termid = trm.id
LEFT JOIN TEMP_GRADESCALEITEM gsi ON sg.grade_scale_id = gsi.gradescale_id
LEFT JOIN TEMP_S_TX_GSI_X xtgsi ON sg.grade_scale_item_id = xtgsi.gradescaleitemid
LEFT JOIN TEMP_COURSESCOREFIELDS ccf ON sg.course_number = ccf.course_number
LEFT JOIN TEMP_S_TX_SERVICE_ID_S svc ON ccf.alt_course_number = svc.service_id AND svc.yearid = trm.yearid;


