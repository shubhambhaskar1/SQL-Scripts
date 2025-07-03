WITH TEMP_INCIDENT AS (
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_INCIDENT
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_INCIDENT_DETAIL AS (
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_INCIDENT_DETAIL
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_INCIDENT_LU_CODE AS (
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_INCIDENT_LU_CODE
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_INCIDENT_LU_SUB_CODE AS (
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_INCIDENT_LU_SUB_CODE
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_S_TX_INC_X AS (
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_S_TX_INC_X
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
),

TEMP_INCIDENT_ACTION_ATTRIBUTE AS (
    SELECT * 
    FROM $source_lakehouse_name.dbo.PS_INCIDENT_ACTION_ATTRIBUTE
    WHERE _META_SCHOOL_YEAR >= '{SchoolYearLowerLimit}'
      AND _META_SCHOOL_YEAR <= '{SchoolYearUpperLimit}'
)

SELECT 
    inc.school_id 											                                                              AS schoolId,                                          
    stx.state_incident_id 									                                                      		AS incidentIdentifier,                        
    inc.incident_ts 												                                                          AS incidentDate,                                    
    SUBSTRING(inc.incident_title || '.  ' || SUBSTRING(inc.incident_detail_desc, 1, 252), 1, 255) 		AS incidentDescription,  
    CASE 
        WHEN ilc.code_type = 'locationcode' THEN ils.state_detail_report_code
        ELSE NULL
    END 												                                                                     	AS incidentLocation,
    CASE 
        WHEN ilc.code_type = 'behaviorcode' THEN ils.state_detail_report_code
        ELSE NULL
    END 													                                                                    AS behaviorDescriptor,
    CASE 
        WHEN ilc.code_type = 'actionattribute'
             AND ilc.state_aggregate_rpt_code = 'SafeSuppSchPrgReview'
             AND ils.lu_code_id = ilc.lu_code_id
             AND ilc.lu_code_id = ind.lu_code_id
             AND ils.sub_category = iaa.text_attribute
             AND iaa.incident_detail_id = ind.incident_detail_id
        THEN ils.state_detail_report_code
        ELSE NULL
    END 													                                                                   AS tx_safeSupportiveSchoolProgramTeamReview
FROM TEMP_INCIDENT inc
LEFT JOIN TEMP_INCIDENT_DETAIL ind
    ON inc.incident_id = ind.incident_id

LEFT JOIN TEMP_INCIDENT_LU_CODE ilc
    ON ind.lu_code_id = ilc.lu_code_id

LEFT JOIN TEMP_INCIDENT_LU_SUB_CODE ils
    ON ilc.lu_code_id = ils.lu_code_id

LEFT JOIN TEMP_INCIDENT_ACTION_ATTRIBUTE iaa
    ON inc.incident_id = iaa.incident_id
AND ind.incident_detail_id=iaa.incident_detail_id

LEFT JOIN TEMP_S_TX_INC_X stx
    ON inc.INCIDENT_ID = stx.INCIDENTINCIDENT_ID
