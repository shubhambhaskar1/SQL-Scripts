WITH TEMP_ATTENDANCE AS (
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_ATTENDANCE
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_ATTENDANCE_CODE AS (
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_ATTENDANCE_CODE
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_CALENDAR_DAY AS (
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_CALENDAR_DAY
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_BELL_SCHEDULE_ITEMS AS (
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_BELL_SCHEDULE_ITEMS
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_S_TX_BSI_X AS (
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_S_TX_BSI_X
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
)
SELECT
    att.event_date 												AS eventDate,                                            
    att.schoolid 												AS schoolId,                                              
    att.p_yearid + 1991 											AS schoolYear,                                        
    TO_CHAR(t.schoolid) || '-' || REPLACE(TRIM(t.abbreviation), CHR(9), '') || '-' || TO_CHAR(t.dcid) 		AS sessionName,
    att.studentid 												AS studentUniqueId,                                 
 
    TO_CHAR(SUBSTR(att.att_comment, 1, 40)) 									AS attendanceEventReason,         
   --- bsix.percent_of_day 											AS eventDuration,
sch.school_name 												as school_name,
s.student_number 												as student_number,
s.lastfirst 													as lastfirst                                      


FROM TEMP_ATTENDANCE att

LEFT JOIN TEMP_ATTENDANCE_CODE ac
    ON att.attendance_codeid = ac.id

LEFT JOIN $source_lakehouse_name.dbo.PS_TERMS t
    ON att.schoolid = t.schoolid
AND att.yearid=t.yearid

LEFT JOIN TEMP_BELL_SCHEDULE_ITEMS bsi
    ON att.att_code = bsi.DAILY_ATTENDANCE_CODE

LEFT JOIN TEMP_S_TX_BSI_X bsix
    ON bsi.dcid = bsix.BELL_SCHEDULE_ITEMSDCID

LEFT JOIN $source_lakehouse_name.dbo.PS_SCHOOLS sch
    ON att.schoolid = sch.schoolid


LEFT JOIN $source_lakehouse_name.dbo.PS_STUDENTS st
    ON att.studentid=st.id
