-- The first CTE build the frame for patients entering and exiting the cohort. This frame is based on NCD forms with visit types of 'initial visit' and 'discharge visit'. The query takes all initial visit dates and matches discharge visit dates if the discharge visit date falls between the initial visit date and the next initial visit date (if present).
WITH initial AS (
	SELECT 
		patient_id, encounter_id AS initial_encounter_id, visit_location AS initial_visit_location, date AS initial_visit_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS initial_visit_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS next_initial_visit_date
	FROM hepatitis_c WHERE visit_type = 'Initial visit'),
cohort AS (
	SELECT
		i.patient_id, i.initial_encounter_id, i.initial_visit_location, i.initial_visit_date, CASE WHEN i.initial_visit_order > 1 THEN 'Yes' END readmission, d.encounter_id AS discharge_encounter_id, d.date AS discharge_date, d.patient_outcome, d.hcv_pcr_12_weeks_after_treatment_end, d.date_test_completed, d.result_return_date
	FROM initial i
	LEFT JOIN (SELECT patient_id, date, encounter_id, patient_outcome, hcv_pcr_12_weeks_after_treatment_end, date_test_completed, result_return_date FROM hepatitis_c WHERE visit_type = 'Discharge visit') d 
		ON i.patient_id = d.patient_id AND d.date >= i.initial_visit_date AND (d.date < i.next_initial_visit_date OR i.next_initial_visit_date IS NULL)),
-- The last Hepatitis C visit CTE extracts the last visit data per cohort enrollment to look at if there are values reported for illicit drug use, pregnancy, hospitalisation, jaundice, hepatic encephalopathy, ascites, haematemesis, or cirrhosis repoted at the last visit. 
last_hepc_visit AS (
SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		hc.date::date AS last_visit_date,
		hc.visit_type AS last_visit_type,
		hc.illicit_drug_use AS drug_use_last_visit,
		hc.currently_pregnant AS pregnant_last_visit,
		hc.hospitalised_since_last_visit AS hospitalised_last_visit,
		hc.jaundice AS jaundice_last_visit,
		hc.hepatic_encephalopathy AS hepatic_encephalopathy_last_visit,
		hc.ascites AS ascites_last_visit,
		hc.haematemesis AS haematememesis_last_visit,
		hc.clinical_decompensated_cirrhosis AS cirrhosis_last_visit
	FROM cohort c
	LEFT OUTER JOIN hepatitis_c hc
		ON c.patient_id = hc.patient_id AND c.initial_visit_date <= hc.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= hc.date::date
	ORDER BY c.patient_id, c.initial_encounter_id, hc.patient_id, hc.date::date DESC),		
-- The initial treatment CTE extracts treatment start data from the initial visit per cohort enrollment. If multiple initial visits have treatment initiation data, the most recent one is reported. 
treatment_initial AS (
SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		hc.date::date,
		hc.visit_type,
		hc.treatment_start_date,
		hc.medication_duration,
		hc.hepatitis_c_treatment_choice,
		hc.treatment_end_date
	FROM cohort c
	LEFT OUTER JOIN hepatitis_c hc
		ON c.patient_id = hc.patient_id AND c.initial_visit_date <= hc.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= hc.date::date
	WHERE hc.treatment_start_date IS NOT NULL AND hc.visit_type = 'Initial visit'
	ORDER BY c.patient_id, c.initial_encounter_id, hc.patient_id, hc.date::date DESC),
-- The follow-up treatment CTE extracts treatment start data from the initial visit per cohort enrollment. If multiple initial visits have treatment initiation data, the most recent one is reported. 
treatment_secondary AS (
SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id) c.patient_id,
		c.initial_encounter_id,
		c.initial_visit_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		hc.date::date,
		hc.visit_type,
		hc.treatment_start_date,
		hc.medication_duration,
		hc.hepatitis_c_treatment_choice,
		hc.treatment_end_date
	FROM cohort c
	LEFT OUTER JOIN hepatitis_c hc
		ON c.patient_id = hc.patient_id AND c.initial_visit_date <= hc.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= hc.date::date
	WHERE hc.treatment_start_date IS NOT NULL AND hc.visit_type = 'Follow up visit'
	ORDER BY c.patient_id, c.initial_encounter_id, hc.patient_id, hc.date::date DESC),
-- The last visit location CTE finds the last visit location reported in hepatitis C forms.
last_visit_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date) c.initial_encounter_id,
		hc.visit_location AS last_visit_location
	FROM cohort c
	LEFT OUTER JOIN hepatitis_c hc
		ON c.patient_id = hc.patient_id AND c.initial_visit_date <= hc.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= hc.date::date
	WHERE hc.visit_location IS NOT NULL
	GROUP BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, hc.date, hc.visit_location
	ORDER BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, hc.date DESC)
-- Main query --
SELECT
	pi."Patient_Identifier",
	c.patient_id,
	c.initial_encounter_id,
	pa."Other_patient_identifier",
	pa."Previous_MSF_code",
	pdd.age AS age_current,
	CASE 
		WHEN pdd.age::int <= 3 THEN '0-3'
		WHEN pdd.age::int >= 4 AND pdd.age::int <= 7 THEN '04-07'
		WHEN pdd.age::int >= 8 AND pdd.age::int <= 14 THEN '08-14'
		WHEN pdd.age::int >= 15 AND pdd.age::int <= 17 THEN '15-17'
		WHEN pdd.age::int >= 18 THEN '18+'
		ELSE NULL
	END AS age_group_current,
	EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) AS age_admission,
	CASE 
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int <= 3 THEN '0-3'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 4 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 7 THEN '04-07'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 8 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 14 THEN '08-14'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 15 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 17 THEN '15-17'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 18 THEN '18+'
		ELSE NULL
	END AS age_group_admission,
	pdd.gender,
	pa."patientCity" AS camp_location, 
	pa."Legal_status",
	pa."Civil_status",
	pa."Education_level",
	pa."Occupation",
	pa."Personal_Situation",
	pa."Living_conditions",
	c.initial_visit_date AS enrollment_date,
	CASE WHEN c.discharge_date IS NULL THEN 'Yes' END AS in_cohort,
	c.readmission,
	c.initial_visit_location,
	lvl.last_visit_location,
	c.discharge_date,
	c.patient_outcome,
	lhv.last_visit_date,
	lhv.last_visit_type,
	lhv.drug_use_last_visit,
	lhv.pregnant_last_visit,
	lhv.hospitalised_last_visit,
	lhv.jaundice_last_visit,
	lhv.hepatic_encephalopathy_last_visit,
	lhv.ascites_last_visit,
	lhv.haematememesis_last_visit,
	lhv.cirrhosis_last_visit,
	ti.treatment_start_date AS treatment_start_date_initial,
	ti.medication_duration AS treatment_duration_initial,
	ti.hepatitis_c_treatment_choice AS treatment_initial,
	ti.treatment_end_date AS treatment_end_date_initial,
	ts.treatment_start_date AS treatment_start_date_fu,
	ts.medication_duration AS treatment_duration_fu,
	ts.hepatitis_c_treatment_choice AS treatment_fu,
	ts.treatment_end_date AS treatment_end_date_fu
FROM cohort c
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN person_attributes pa
	ON c.patient_id = pa.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON c.initial_encounter_id = ped.encounter_id
LEFT OUTER JOIN last_hepc_visit lhv	
	ON c.initial_encounter_id = lhv.initial_encounter_id
LEFT OUTER JOIN treatment_initial ti
	ON c.initial_encounter_id = ti.initial_encounter_id
LEFT OUTER JOIN treatment_secondary ts
	ON c.initial_encounter_id = ts.initial_encounter_id
LEFT OUTER JOIN last_visit_location lvl
	ON c.initial_encounter_id = lvl.initial_encounter_id;