TEMP_STUDENTS AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_STUDENTS
    WHERE DCID RLIKE '^[0-9]+$'
    AND _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_STU_X AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_S_TX_STU_X
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_GEN
    SELECT * FROM $source_lakehouse_name.dbo.PS_S_TX_STU_STATE_GEN_C
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_STU_COREFIELDS AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_StudentCoreFields
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_ECON AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_S_TX_SEN_ECON_X
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_ESL AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_S_TX_SEN_ESL_X
    WHERE META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_STUDENTRACE AS(
    SELECT * FROM $source_lakehouse_name.dbo.PS_STUDENTRACE
    WHERE DCID RLIKE '^[0-9]+$'
    AND _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
    AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),


SELECT
    stu.state_studentnumber                                                                                             AS studentUniqueId,
    '101912'                                                                                                            AS educationOrganizationId,
    null                                                                                                                AS addressTypeDescriptor     -- Not sure which to put out of 01 Home and 04 Mailing
    COALESCE(NULLIF(stu.street, ''), NULLIF(stu.mailing_street, ''))                                                    AS streetNumberName,
    COALESCE(
        TRIM(REGEXP_SUBSTR(stu.street, '[^,]+', 1, 2)),
        TRIM(REGEXP_SUBSTR(stu.mailing_street, '[^,]+', 1, 2))
    )                                                                                                                   AS apartmentRoomSuiteNumber,
    COALESCE(NULLIF(stu.city, ''), NULLIF(stu.mailing_city, ''))                                                        AS city,
    COALESCE(NULLIF(stu.state, ''), NULLIF(stu.mailing_state, ''))                                                      AS stateAbbreviationDescriptor,
    COALESCE(NULLIF(stu.zip, ''), NULLIF(stu.mailing_zip, ''))                                                          AS postalCode,
    'Twelfth grade'                                                                                                     AS cohortYearTypeDescriptor,
    coasesce(studentcorefields.graduation_year, s.classof)                                                              AS schoolYear,
    null                                                                                                                AS electronicMailAddress,      -- Did not found any table called - PSM_STUDENTCONTACT.email
    null                                                                                                                AS electronicMailTypeDescriptor, -- -- Not sure about what to take as a input/Source Home/Personal
    COALESCE(NULLIF(stu.gender, ''), NULLIF(stu_core.pscore_legal_gender, ''))                                          AS sexDescriptor,
    stu.fedethnicity                                                                                                    AS hispanicLatinoEthnicity,
    COALESCE(NULLIF(stu_core.primarylanguage, ''), NULLIF(stu_core.secondarylanguage, ''))                              AS LanguageDescriptor,
    CASE 
      WHEN studentcorefields.primarylanguage is not null THEN 01 
      ELSE 02 
    END                                                                                                                 AS languageUseDescriptor,
    stu_r.RaceCd                                                                                                        AS raceDescriptor,
    econ.E0785_ECON_DISADVANTAGED AS economicDisadvantageDescriptor,
    esl.E0790_LEP_Indicator AS emergentBilingualIndicatorDescriptor
FROM TEMP_STUDENTS stu
LEFT JOIN TEMP_STU_X stux ON stu.DCID = stux.StudentsDCID
LEFT JOIN TEMP_GEN gen ON stu.DCID = gen.StudentsDCID
LEFT JOIN TEMP_STU_COREFIELDS stu_core ON stu.DCID = stu_core.studentsdcid
LEFT JOIN TEMP_ECON econ ON stu.DCID = econ.studentsdcid
LEFT JOIN TEMP_ESL esl ON stu.DCID = esl.studentsdcid
LEFT JOIN TEMP_STUDENTRACE stu_r ON stu.DCID = stu_r.DCID
