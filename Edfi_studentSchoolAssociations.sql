control: 
    refresh_frequency_hours: 1
    refresh_frequency_days: 0.5
    buffer_period_days: 5         # Requires Watermark Column
    priority: 1
destination:
    write_type: "overwrite"
    format: "json"
    relative_path: "ed-fi/studentSchoolAssociations"
    watermark_column: "_META_RUNDATE"
sql:
    edfi_student_school_associations_step1:
        statement: |
            WITH TEMP_S_TX_STU_SCHOOL_ENR_C_RAW AS (
                SELECT *,
                    ROW_NUMBER() over (PARTITION BY StudentsDCID ORDER BY EntryDate DESC) AS rn  -- To get the most recent enrollment
                 FROM $source_lakehouse_name.dbo.PS_S_TX_STU_SCHOOL_ENR_C
                WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
                AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}' 
            ),

            TEMP_S_TX_STU_SCHOOL_ENR_C AS (
                SELECT * 
                FROM TEMP_S_TX_STU_SCHOOL_ENR_C_RAW
                WHERE RN = 1 OR EntryDate <= CURREN_DATE()
            ),

            TEMP_STUDENTS AS(
                SELECT * FROM $source_lakehouse_name.dbo.PS_STUDENTS
                WHERE DCID RLIKE '^[0-9]+$'
                AND _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
                AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
            ),

            TEMP_S_STU_X AS(
                SELECT * FROM $source_lakehouse_name.dbo.PS_S_STU_X
            ),

            TEMP_TERMS AS (
                SELECT 
                    firstday,
                    lastday,
                    YearID   -- Need to check what column name should be used for Year
            )

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

            
            WHERE 
                -- student not exluded from Ed-Fi (S_STU_X.ExcludeFromEdFi = 1)
                COALESCE(stux.ExcludeFromEdFi,0) != 1 

                -- enrollment is not a pre-registration (s.enroll_status = -1)
                AND stu.enroll_status != -1

                -- exit date is greater than entry date
                AND enr.EntryDate < enr.ExitDate
                AND enr.EntryDate IS NOT NULL

                -- Entry date within the term
                AND enr.EntryDate BETWEEN 
                    (SELECT MIN(firstday) FROM TEMP_TERMS WHERE YearID = enr.SchoolYear)
                    AND
                    (SELECT MAX(firstday) FROM TEMP_TERMS WHERE YearID = enr.SchoolYear)


            
            

        cacheable: true
            
