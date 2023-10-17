-- The first 3 sub-tables of the query build the frame for monitoring patient entry to and exit from the waiting list and cohort. The table is built by listing all intake dates as recorded in the MH intake form. For each exit date is matched to an intake following the logic that is occurs after the entry date but not before the next entry date, in the case that a patient enters the cohort more than once.
WITH intake_cte_2 AS (
	SELECT patient_id, encounter_id AS entry_encounter_id, date::date AS intake_date, CONCAT(patient_id, ROW_NUMBER () OVER (PARTITION BY patient_id ORDER BY date)) AS entry_id_2, 1 AS one
	FROM mental_health_intake),
intake_cte_1 AS (
	SELECT patient_id, entry_encounter_id, intake_date, entry_id_2::int+one AS entry_id_1
	FROM intake_cte_2),
entry_exit_cte AS (
	SELECT ic1.patient_id, ic1.entry_encounter_id, ic1.intake_date, mhd.discharge_date::date, mhd.encounter_id AS discharge_encounter_id
	FROM intake_cte_1 ic1
	LEFT OUTER JOIN intake_cte_2 ic2
		ON ic1.entry_id_1::int = ic2.entry_id_2::int
	LEFT OUTER JOIN (SELECT patient_id, discharge_date, encounter_id FROM mental_health_discharge) mhd
		ON ic1.patient_id = mhd.patient_id AND mhd.discharge_date >= ic1.intake_date AND (mhd.discharge_date < ic2.intake_date OR ic2.intake_date IS NULL)),
-- The first psy initial assessment table extracts the date from the psy initial assessment as the cohort entry date. If multiple initial assessments are completed then the first is used.
first_psy_initial_assessment AS (
SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date) eec.entry_encounter_id,
		pcia.date::date
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psy_counselors_initial_assessment pcia
		ON eec.patient_id = pcia.patient_id
	WHERE pcia.date >= eec.intake_date AND (pcia.date <= eec.discharge_date OR eec.discharge_date IS NULL)
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date, pcia.date
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date, pcia.date ASC),
-- The first clinician initial assessment table extracts the date from the first clinician initial assesment. If multiple clinician initial assessments are completed then the first is used. This table is used in case there is no psy initial assessment date provided. 
first_clinician_initial_assessment AS (
SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date) eec.entry_encounter_id,
		pmia.date::date
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psychiatrist_mhgap_initial_assessment pmia 
		ON eec.patient_id = pmia.patient_id
	WHERE pmia.date >= eec.intake_date AND (pmia.date <= eec.discharge_date OR eec.discharge_date IS NULL)
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date, pmia.date
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date, pmia.date ASC),
-- The visit location sub-table finds the last visit location reported across all clinical consultaiton/session forms.
last_visit_location_cte AS (	
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date) eec.entry_encounter_id,
		vlc.visit_location AS visit_location
	FROM entry_exit_cte eec
	LEFT OUTER JOIN (
		SELECT pcia.date::date, pcia.patient_id, pcia.visit_location FROM psy_counselors_initial_assessment pcia WHERE pcia.visit_location IS NOT NULL 
		UNION 
		SELECT pmia.date::date, pmia.patient_id, pmia.visit_location FROM psychiatrist_mhgap_initial_assessment pmia WHERE pmia.visit_location IS NOT NULL 
		UNION
		SELECT pcfu.date::date, pcfu.patient_id, pcfu.visit_location FROM psy_counselors_follow_up pcfu WHERE pcfu.visit_location IS NOT NULL 
		UNION
		SELECT pmfu.date::date, pmfu.patient_id, pmfu.visit_location FROM psychiatrist_mhgap_follow_up pmfu WHERE pmfu.visit_location IS NOT NULL
		UNION
		SELECT mhd.discharge_date AS date, mhd.patient_id, mhd.visit_location FROM mental_health_discharge mhd WHERE mhd.visit_location IS NOT NULL) vlc
		ON eec.patient_id = vlc.patient_id
	WHERE vlc.date >= eec.intake_date AND (vlc.date <= eec.discharge_date OR eec.discharge_date IS NULL)
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date, vlc.date, vlc.visit_location
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date, vlc.date DESC),
-- THe Stressor sub-table creates a vertical list of all stressors.
stressor_vertical_cte AS (
    SELECT 
        encounter_id,
        stressor_1 AS stressor
    FROM mental_health_intake
    UNION
    SELECT
        encounter_id,
        stressor_2 AS stressor
    FROM mental_health_intake
    UNION
    SELECT 
        encounter_id,
        stressor_3 AS stressor
    FROM mental_health_intake),
stressors_cte AS (
	SELECT
		DISTINCT ON (encounter_id, stressor) encounter_id,
		stressor
	FROM stressor_vertical_cte
	where stressor IS NOT NULL)
-- Main query --
SELECT 
	pi."Patient_Identifier",
	eec.patient_id,
	eec.entry_encounter_id,
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
	eec.intake_date, 
	CASE 
		WHEN fpia.date IS NOT NULL THEN fpia.date
		WHEN fpia.date IS NULL THEN fcia.date
		ELSE NULL
	END	AS enrollment_date,
	eec.discharge_date,
	CASE 
		WHEN fpia.date IS NULL AND fcia.date IS NULL AND eec.discharge_date IS NULL THEN 'waiting list'
		WHEN (fpia.date IS NOT NULL OR fcia.date IS NOT NULL) AND eec.discharge_date IS NULL THEN 'in cohort'
		WHEN (fpia.date IS NOT NULL OR fcia.date IS NOT NULL) AND eec.discharge_date IS NOT NULL THEN 'discharge'
	END AS status,	
	mhi.visit_location AS entry_visit_location,
	lvlc.visit_location,
	sc.stressor
FROM stressors_cte sc
LEFT OUTER JOIN entry_exit_cte eec
    ON sc.encounter_id = eec.entry_encounter_id
LEFT OUTER JOIN first_psy_initial_assessment fpia 
	ON eec.entry_encounter_id = fpia.entry_encounter_id
LEFT OUTER JOIN first_clinician_initial_assessment fcia 
	ON eec.entry_encounter_id = fcia.entry_encounter_id
LEFT OUTER JOIN patient_identifier pi
	ON eec.patient_id = pi.patient_id
LEFT OUTER JOIN person_details_default pdd 
	ON eec.patient_id = pdd.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON eec.entry_encounter_id = ped.encounter_id
LEFT OUTER JOIN mental_health_intake mhi
	ON eec.entry_encounter_id = mhi.encounter_id
LEFT OUTER JOIN last_visit_location_cte lvlc
	ON eec.entry_encounter_id = lvlc.entry_encounter_id;