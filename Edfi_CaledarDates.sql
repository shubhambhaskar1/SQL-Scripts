WITH TEMP_CALENDARDAY AS ( 
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_CALENDARDAY
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_S_TX_CLD_C AS (
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_S_TX_CLD_C
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_S_TX_CLD_X AS (
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_S_TX_CLD_X
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_CALENDAR AS (
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_CALENDAR
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_S_TX_SCH_SCHOOLS_X AS (
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_S_TX_SCH_SCHOOLS_X
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_S_TX_SCH_SCHOOLS_C AS (
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_S_TX_SCH_SCHOOLS_C
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
)

SELECT
    CONCAT('ps_', SHA2(CAST(COALESCE(CD.CALENDARDAYID, '') AS STRING), 256)) AS id,
    CD.DATE_VALUE AS date,
    C.CODE AS calendarCode,
    CD.CAMPUSID AS schoolId,
    CD.SCHOOLYEARID + 1991 AS schoolYear,
    S_TX_CLD_X.Waiver_Event_Type_Code AS TX_Waiver_Event_Type_Code_X,
    S_TX_CLD_C.Waiver_Event_Type_Code AS TX_Waiver_Event_Type_Code_C,
    S_TX_SCH_SCHOOLS_X.Instructional_Program_Type AS TX_Instructional_Program_Type_X,
    S_TX_SCH_SCHOOLS_C.Instructional_Program_Type AS TX_Instructional_Program_Type_C,
    COALESCE(S_TX_CLD_X.Waiver_Event_Type_Code, S_TX_CLD_C.Waiver_Event_Type_Code) AS calendarWaiverEventTypeDescriptor,

    CASE
        WHEN S_TX_SCH_SCHOOLS_X.Instructional_Program_Type IN ('01', '02', '14') THEN
            COALESCE(S_TX_CLD_X.School_Day_Minutes, S_TX_CLD_C.School_Day_Minutes, S_TX_SCH_SCHOOLS_X.Default_Day_Minutes, S_TX_SCH_SCHOOLS_C.Default_Day_Minutes)
        WHEN S_TX_SCH_SCHOOLS_X.Instructional_Program_Type IN ('03','04','05','06','07','08','09','10','11','12','15','16') THEN
            COALESCE(S_TX_CLD_X.School_Day_Minutes, S_TX_CLD_C.School_Day_Minutes, S_TX_SCH_SCHOOLS_X.Default_Day_Minutes, S_TX_SCH_SCHOOLS_C.Default_Day_Minutes)
        WHEN S_TX_SCH_SCHOOLS_X.Instructional_Program_Type = '13' THEN
            NULL
        ELSE
            NULL
    END AS schoolDayOperationalMinutes,

    CASE
        WHEN S_TX_SCH_SCHOOLS_X.Instructional_Program_Type IN ('03','04','05','06','07','08','09','10','11','12','15','16') THEN
            COALESCE(S_TX_CLD_X.School_Day_Minutes, S_TX_CLD_C.School_Day_Minutes, S_TX_SCH_SCHOOLS_X.Default_Day_Minutes, S_TX_SCH_SCHOOLS_C.Default_Day_Minutes)
        ELSE
            NULL
    END AS schoolDayInstructionalMinutes,

    COALESCE(S_TX_CLD_X.Waiver_Minutes, S_TX_CLD_C.Waiver_Minutes) AS schoolDayWaiverMinutes,

    GREATEST(CD.rundate, C.rundate, S_TX_CLD_C.rundate, S_TX_CLD_X.rundate) AS _META_RUNDATE,
    CD._META_SCHOOL_YEAR AS _META_SCHOOL_YEAR,
    CD._META_DISTRICT_ID AS _META_DISTRICT_ID

FROM TEMP_CALENDARDAY CD

LEFT JOIN TEMP_CALENDAR C
    ON CD.CALENDARID = C.CALENDARID

LEFT JOIN TEMP_S_TX_CLD_X S_TX_CLD_X
    ON CD.CALENDARID = S_TX_CLD_X.CALENDARID

LEFT JOIN TEMP_S_TX_CLD_C S_TX_CLD_C
    ON CD.CALENDARID = S_TX_CLD_C.CALENDARID

LEFT JOIN TEMP_S_TX_SCH_SCHOOLS_X S_TX_SCH_SCHOOLS_X
    ON CD.SCHOOLID = S_TX_SCH_SCHOOLS_X.SCHOOLID

LEFT JOIN TEMP_S_TX_SCH_SCHOOLS_C S_TX_SCH_SCHOOLS_C
    ON CD.SCHOOLID = S_TX_SCH_SCHOOLS_C.SCHOOLID
;
