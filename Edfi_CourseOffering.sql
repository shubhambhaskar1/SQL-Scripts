WITH TEMP_SECTIONS AS (
    SELECT *
    FROM $source_lakehouse_name.dbo.PS_SECTIONS
    WHERE _META_SCHOOL_YEAR BETWEEN '{SchoolYearLowerLimit}' AND '{SchoolYearUpperLimit}'
),

TEMP_COURSES AS (
    SELECT *
    FROM $source_lakehouse_name.dbo.PS_COURSES
    WHERE _META_SCHOOL_YEAR BETWEEN '{SchoolYearLowerLimit}' AND '{SchoolYearUpperLimit}'
),

TEMP_TERMS AS (
    SELECT *
    FROM $source_lakehouse_name.dbo.PS_TERMS
    WHERE _META_SCHOOL_YEAR BETWEEN '{SchoolYearLowerLimit}' AND '{SchoolYearUpperLimit}'
),

TEMP_COURSECOREFIELDS AS (
    SELECT *
    FROM $source_lakehouse_name.dbo.PS_COURSECOREFIELDS
    WHERE _META_SCHOOL_YEAR BETWEEN '{SchoolYearLowerLimit}' AND '{SchoolYearUpperLimit}'
),

TEMP_STOREDGRADES AS (
    SELECT *
    FROM $source_lakehouse_name.dbo.PS_STOREDGRADES
    WHERE _META_SCHOOL_YEAR BETWEEN '{SchoolYearLowerLimit}' AND '{SchoolYearUpperLimit}'
),

TEMP_S_TX_SGR_X AS (
    SELECT *
    FROM $source_lakehouse_name.dbo.PS_S_TX_SGR_X
    WHERE _META_SCHOOL_YEAR BETWEEN '{SchoolYearLowerLimit}' AND '{SchoolYearUpperLimit}'
)

SELECT
    CONCAT('ps_', SHA2(CAST(COALESCE(sec.DCID, '') AS STRING), 256))                          AS id,                          -- Unique Ed-Fi ID
    sec.SchoolID                                                                              AS schoolId,                    -- E0266: CAMPUS-ID
    COALESCE(sgr.course_number, sgrx.Alt_Course_Number, crs.course_number)                    AS localCourseCode,             -- E1194: LOCAL-COURSE-CODE
    COALESCE(sgrx.Alt_Course_Number, ccf.alt_course_number)                                   AS courseCode,                  -- E0724: SERVICE-ID
    sec.SchoolID                                                                              AS schoolIdentity,              -- E0266: Campus ID
    t.YearID                                                                                  AS schoolYear,                  -- E1093: SCHOOL-YEAR
    CONCAT(t.Portion, ' ', t.Abbreviat)                                                       AS sessionName,                 -- E3057: Session Name
    crs.Course_Name                                                                           AS localCourseTitle,            -- Descriptive course title

    CASE
        WHEN LEFT(COALESCE(sgrx.Alt_Course_Number, ccf.alt_course_number), 1) = '8' THEN 1
        ELSE sec.DistrictID
    END                                                                                       AS educationOrganizationId,     -- Education Organization ID

    sec.course_number                                                                         AS courseNumber,                -- Course number from sections
    sec.section_number                                                                        AS sectionNumber,               -- Section number from sections
    sec.termid                                                                                AS termId,                      -- Term ID from sections
    crs.Course_Name                                                                           AS courseName,                  -- Course name from courses
    crs.Alt_Course_Number                                                                     AS altCourseNumber,             -- Alternate course number
    t.firstday                                                                                AS termStartDate,               -- Term start date
    t.lastday                                                                                 AS termEndDate,                 -- Term end date
  
    -- Meta fields for data lineage
    sec._META_SCHOOL_YEAR                                                                     AS _META_SCHOOL_YEAR,
    sec._META_DISTRICT_ID                                                                     AS _META_DISTRICT_ID

FROM TEMP_SECTIONS sec

LEFT JOIN TEMP_COURSES crs
    ON sec.course_number = crs.course_number

LEFT JOIN TEMP_TERMS t
    ON sec.termid = t.id

LEFT JOIN TEMP_COURSECOREFIELDS ccf
    ON crs.DCID = ccf.CoursesDCID

LEFT JOIN TEMP_S_TX_SGR_X sgrx
    ON sec.DCID = sgrx.SectionsDCID

LEFT JOIN TEMP_STOREDGRADES sgr
    ON sec.DCID = sgr.SectionsDCID
;