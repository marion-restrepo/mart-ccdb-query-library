-- The first CTE build the frame for patients entering and exiting the cohort. This frame is based on the MH intake form and the MH discharge form. The query takes all intake dates and matches discharge dates if the discharge date falls between the intake date and the next intake date (if present).
WITH intake AS (
	SELECT 
		patient_id, encounter_id AS intake_encounter_id, date::date AS intake_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS intake_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS next_intake_date
	FROM mental_health_intake),
cohort AS (
	SELECT
		i.patient_id, i.intake_encounter_id, i.intake_date, CASE WHEN i.intake_order > 1 THEN 'Yes' END readmission, mhd.encounter_id AS discharge_encounter_id, mhd.discharge_date::date
	FROM intake i
	LEFT JOIN mental_health_discharge mhd 
		ON i.patient_id = mhd.patient_id AND mhd.discharge_date >= i.intake_date AND (mhd.discharge_date < i.next_intake_date OR i.next_intake_date IS NULL)),
-- The first psy initial assessment table extracts the date from the psy initial assessment as the cohort entry date. If multiple initial assessments are completed then the first is used.
first_psy_initial_assessment AS (
SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		pcia.date::date
	FROM cohort c
	LEFT OUTER JOIN psy_counselors_initial_assessment pcia
		ON c.patient_id = pcia.patient_id
	WHERE pcia.date >= c.intake_date AND (pcia.date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pcia.date
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pcia.date ASC),
-- The first clinician initial assessment table extracts the date from the first clinician initial assesment. If multiple clinician initial assessments are completed then the first is used. This table is used in case there is no psy initial assessment date provided. 
first_clinician_initial_assessment AS (
SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		pmia.date::date
	FROM cohort c
	LEFT OUTER JOIN psychiatrist_mhgap_initial_assessment pmia 
		ON c.patient_id = pmia.patient_id
	WHERE pmia.date >= c.intake_date AND (pmia.date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pmia.date
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pmia.date ASC),
-- The visit location sub-table finds the last visit location reported across all clinical consultaiton/session forms.
last_visit_location_cte AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		vlc.visit_location AS visit_location
	FROM cohort c
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
		ON c.patient_id = vlc.patient_id
	WHERE vlc.date >= c.intake_date AND (vlc.date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, vlc.date, vlc.visit_location
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, vlc.date DESC),
-- The Mental Health syndrome CTEs takes only the last diagnoses reported per cohort enrollment from either the Psychiatrist mhGap initial or follow-up forms. 
ia_syndrome AS (
	SELECT 
        pcia.patient_id, c.intake_encounter_id, pcia.date, pcia.main_syndrome, pcia.additional_syndrome, ROW_NUMBER() OVER (PARTITION BY pcia.patient_id ORDER BY pcia.date DESC, pcia.date_created DESC) AS rn
    FROM psy_counselors_initial_assessment pcia
	JOIN cohort c
		ON pcia.patient_id = c.patient_id AND c.intake_date <= pcia.date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= pcia.date
    WHERE COALESCE(pcia.main_syndrome, pcia.additional_syndrome) IS NOT NULL),
last_syndrome AS (
	SELECT
		patient_id, intake_encounter_id, date, main_syndrome AS syndrome
	FROM ia_syndrome
	WHERE main_syndrome IS NOT NULL AND rn = 1
	UNION ALL
	SELECT
		patient_id, intake_encounter_id, date, additional_syndrome AS syndrome
	FROM ia_syndrome
	WHERE additional_syndrome IS NOT NULL AND rn = 1)
-- Main query --
SELECT 
	pi."Patient_Identifier",
	c.patient_id,
	c.intake_encounter_id,
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
	c.intake_date, 
	CASE 
		WHEN fpia.date IS NOT NULL AND fcia.date IS NULL THEN fpia.date
		WHEN fcia.date IS NOT NULL AND fpia.date IS NULL THEN fcia.date
		WHEN fpia.date IS NOT NULL AND fcia.date IS NOT NULL AND fcia.date::date <= fpia.date::date THEN fcia.date
		WHEN fpia.date IS NOT NULL AND fcia.date IS NOT NULL AND fcia.date::date > fpia.date::date THEN fpia.date
		ELSE NULL
	END	AS enrollment_date,
	c.discharge_date,
	CASE 
		WHEN fpia.date IS NULL AND fcia.date IS NULL AND c.discharge_date IS NULL THEN 'waiting list'
		WHEN (fpia.date IS NOT NULL OR fcia.date IS NOT NULL) AND c.discharge_date IS NULL THEN 'in cohort'
		WHEN (fpia.date IS NOT NULL OR fcia.date IS NOT NULL) AND c.discharge_date IS NOT NULL THEN 'discharge'
	END AS status,	
	mhi.visit_location AS entry_visit_location,
	lvlc.visit_location,
	ls.syndrome
FROM last_syndrome ls
LEFT OUTER JOIN cohort c
    ON ls.intake_encounter_id = c.intake_encounter_id
LEFT OUTER JOIN first_psy_initial_assessment fpia 
	ON c.intake_encounter_id = fpia.intake_encounter_id
LEFT OUTER JOIN first_clinician_initial_assessment fcia 
	ON c.intake_encounter_id = fcia.intake_encounter_id
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON c.intake_encounter_id = ped.encounter_id
LEFT OUTER JOIN mental_health_intake mhi
	ON c.intake_encounter_id = mhi.encounter_id
LEFT OUTER JOIN last_visit_location_cte lvlc
	ON c.intake_encounter_id = lvlc.intake_encounter_id;