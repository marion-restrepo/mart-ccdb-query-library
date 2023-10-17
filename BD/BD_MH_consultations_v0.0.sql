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
		ON ic1.patient_id = mhd.patient_id AND mhd.discharge_date > ic1.intake_date AND (mhd.discharge_date < ic2.intake_date OR ic2.intake_date IS NULL)),
-- The Consulations sub-table creates a master table of all consultations/sessions as reported by the clinical forms.
consultations_cte AS (
	SELECT
		pcia.date::date,
		pcia.patient_id,
		pcia.visit_location,
		pcia.intervention_setting,
		'Individual session' AS type_of_activity,
		'Initial' AS visit_type,
		'Counselor' AS provider_type,
		pcia.encounter_id
	FROM psy_counselors_initial_assessment pcia 
	UNION
	SELECT
		pmia.date::date,
		pmia.patient_id,
		pmia.visit_location,
		pmia.intervention_setting,
		'Individual session' AS type_of_activity,
		'Initial' AS visit_type,
		'Psychiatrist' AS provider_type,
		pmia.encounter_id
	FROM psychiatrist_mhgap_initial_assessment pmia
	UNION
	SELECT
		pcfu.date::date,
		pcfu.patient_id,
		pcfu.visit_location,
		pcfu.intervention_setting,
		pcfu.type_of_activity,
		'Follow up' AS visit_type,
		'Counselor' AS provider_type,
		pcfu.encounter_id
	FROM psy_counselors_follow_up pcfu 
	UNION
	SELECT 
		pmfu.date::date,
		pmfu.patient_id,
		pmfu.visit_location,
		pmfu.intervention_setting,
		pmfu.type_of_activity,
		'Follow up' AS visit_type,
		'Psychiatrist' AS provider_type,
		pmfu.encounter_id
	FROM psychiatrist_mhgap_follow_up pmfu)
-- Main query --
SELECT 
	DISTINCT ON (cc.patient_id, cc.date::date, cc.visit_location, cc.intervention_setting, cc.type_of_activity, cc.visit_type, cc.provider_type) cc.patient_id,
	pi."Patient_Identifier",
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
	pdd.gender,
	pad.state_province AS governorate, 
	pad.city_village AS community_village,
	pad.address2 AS area,
	cc.date::date AS visit_date,
	cc.visit_location,
	cc.intervention_setting,
	cc.type_of_activity,
	cc.visit_type,
	cc.provider_type,
	cc.encounter_id
FROM consultations_cte cc
LEFT OUTER JOIN entry_exit_cte eec
	ON cc.patient_id = eec.patient_id AND cc.date >= eec.intake_date AND (cc.date <= eec.discharge_date OR eec.discharge_date is NULL)
LEFT OUTER JOIN patient_identifier pi
	ON cc.patient_id = pi.patient_id
LEFT OUTER JOIN person_details_default pdd 
	ON cc.patient_id = pdd.person_id
LEFT OUTER JOIN person_address_default pad
	ON eec.patient_id = pad.person_id;