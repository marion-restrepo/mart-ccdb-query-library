-- The first CTE build the frame for patients entering and exiting the cohort. This frame is based on MNT VIH TB form with visit types of 'Inclusion' and 'Sortie'. The query takes all initial visit dates and matches discharge visit dates if the discharge visit date falls between the initial visit date and the next initial visit date (if present).
WITH inclusion AS (
	SELECT 
		patient_id, encounter_id AS encounter_id_inclusion, lieu_de_visite AS lieu_de_visite_inclusion, date AS date_inclusion, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS inclusion_visit_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS date_inclusion_suivi
	FROM mnt_vih_tb WHERE type_de_visite = 'Inclusion'),
cohorte AS (
	SELECT
		i.patient_id, i.encounter_id_inclusion, i.lieu_de_visite_inclusion, i.date_inclusion, CASE WHEN i.inclusion_visit_order > 1 THEN 'Oui' END readmission, d.encounter_id AS encounter_id_sortie, CASE WHEN d.date_de_sortie IS NOT NULL THEN d.date_de_sortie WHEN d.date_de_sortie IS NULL THEN d.date ELSE NULL END AS date_de_sortie, d.statut_de_sortie AS statut_de_sortie
	FROM inclusion i
	LEFT JOIN (SELECT patient_id, date, encounter_id, date_de_sortie, statut_de_sortie FROM mnt_vih_tb WHERE type_de_visite = 'Sortie') d 
		ON i.patient_id = d.patient_id AND d.date >= i.date_inclusion AND (d.date < i.date_inclusion_suivi OR i.date_inclusion_suivi IS NULL)),
-- The diagnosis CTEs select the last reported NCD diagnosis per cohort enrollment and pivots the data horizontally.
diagnostic_cohorte AS (
	SELECT
		d.patient_id, c.encounter_id_inclusion, n.date, d.diagnostic
	FROM diagnostic d 
	LEFT JOIN mnt_vih_tb n USING(encounter_id)
	LEFT JOIN cohorte c ON d.patient_id = c.patient_id AND c.date_inclusion <= n.date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE current_date END >= n.date),
dernière_diagnostic_cohorte AS (
	SELECT cd.patient_id, cd.encounter_id_inclusion, cd.date, cd.diagnostic
	FROM diagnostic_cohorte cd
	INNER JOIN (SELECT encounter_id_inclusion, MAX(date) AS max_date FROM diagnostic_cohorte GROUP BY encounter_id_inclusion) cd2 ON cd.encounter_id_inclusion = cd2.encounter_id_inclusion AND cd.date = cd2.max_date),
-- The last visit location CTE finds the last visit location reported in clinical forms.
dernière_fiche_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion,
		nvsl.lieu_de_visite AS dernière_fiche_location
	FROM cohorte c
	LEFT OUTER JOIN (SELECT patient_id, CASE WHEN type_de_visite = 'Sortie' AND date_de_sortie IS NOT NULL THEN date_de_sortie ELSE date END AS date, lieu_de_visite FROM mnt_vih_tb UNION 
	SELECT patient_id, date, lieu_de_visite FROM ptpe) nvsl
		ON c.patient_id = nvsl.patient_id AND c.date_inclusion <= nvsl.date::date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE current_date END >= nvsl.date::date
	WHERE nvsl.lieu_de_visite IS NOT NULL
	GROUP BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, nvsl.date, nvsl.lieu_de_visite
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, nvsl.date DESC)
-- Main query -- 
SELECT 
	pi."Patient_Identifier",
	c.patient_id,
	c.encounter_id_inclusion,
	pa."Identifiant_cohorte",
	pdd.age AS age_actuel,
	CASE 
		WHEN pdd.age::int <= 4 THEN '0-4'
		WHEN pdd.age::int >= 5 AND pdd.age::int <= 14 THEN '05-14'
		WHEN pdd.age::int >= 15 AND pdd.age::int <= 24 THEN '15-24'
		WHEN pdd.age::int >= 25 AND pdd.age::int <= 34 THEN '25-34'
		WHEN pdd.age::int >= 35 AND pdd.age::int <= 44 THEN '35-44'
		WHEN pdd.age::int >= 45 AND pdd.age::int <= 54 THEN '45-54'
		WHEN pdd.age::int >= 55 AND pdd.age::int <= 64 THEN '55-64'
		WHEN pdd.age::int >= 65 THEN '65+'
		ELSE NULL
	END AS groupe_age_actuel,
	EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) AS age_inclusion,
	CASE 
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int <= 4 THEN '0-4'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 5 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 14 THEN '05-14'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 15 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 24 THEN '15-24'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 25 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 34 THEN '25-34'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 35 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 44 THEN '35-44'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 45 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 54 THEN '45-54'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 55 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 64 THEN '55-64'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 65 THEN '65+'
		ELSE NULL
	END AS groupe_age_inclusion,
	pdd.gender AS sexe,
	c.date_inclusion AS date_inclusion,
	CASE WHEN c.date_de_sortie IS NULL THEN 'Oui' END AS en_cohorte,
	c.readmission,
	c.lieu_de_visite_inclusion,
	lvl.dernière_fiche_location,	
	c.date_de_sortie,
	c.statut_de_sortie,
	lcd.diagnostic
FROM dernière_diagnostic_cohorte lcd
LEFT OUTER JOIN cohorte c
	ON lcd.encounter_id_inclusion = c.encounter_id_inclusion
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN person_attributes pa
	ON c.patient_id = pa.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON c.encounter_id_inclusion = ped.encounter_id
LEFT OUTER JOIN dernière_fiche_location lvl
	ON c.encounter_id_inclusion = lvl.encounter_id_inclusion;
