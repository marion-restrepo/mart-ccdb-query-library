-- The first CTE build the frame for patients entering and exiting the cohort. This frame is based on MNT VIH TB form with visit types of 'Inclusion' and 'Sortie'. For each patient, the query takes all initial visit dates and matches the discharge visit date occuring after the initial visit. If a patient has multiple initial visits and discharge visits, the match happens sequentially based on the date of visit (e.g. the first initial visit is matched to the first discharge, and so on). If the patient does not have a discharge visit, then the discharge information is empty until completed. 
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
-- The diagnosis CTE selects all reported diagnoses per cohort enrollment, both listing and pivoting the data horizonally. The pivoted diagnosis data is presented with the date the diagnosis was first reported.
diagnostic_cohorte AS (
	SELECT
		DISTINCT ON (d.patient_id, c.encounter_id_inclusion, d.diagnostic_group) d.patient_id, c.encounter_id_inclusion, CASE WHEN d.diagnostic_group = 'mnt' THEN n.date ELSE NULL END AS mnt, CASE WHEN d.diagnostic_group = 'vih' THEN n.date ELSE NULL END AS vih, CASE WHEN d.diagnostic_group = 'tb' THEN n.date ELSE NULL END AS tb, CASE WHEN d.diagnostic_group = 'santé_mentale' THEN n.date ELSE NULL END AS santé_mentale
	FROM (SELECT 
			patient_id, 
			encounter_id, 
			CASE WHEN diagnostic IN ('Asthme','Drépanocytose','Insuffisance renale chronique','Maladie cardiovasculaire','Bronchopneumopathie chronique obstructive','Diabète sucré de type 1','Diabète sucré de type 2','Hypertension','Hypothyroïdie','Hyperthyroïdie','Épilepsie focale','Épilepsie généralisée','Épilepsie non classifiée','Autre') THEN 'mnt' WHEN diagnostic IN ('Tuberculose pulmonaire','Tuberculose extrapulmonaire') THEN 'tb' WHEN diagnostic = 'Infection par le VIH' THEN 'vih' WHEN diagnostic = 'Troubles de santé mentale' THEN 'santé_mentale'	ELSE NULL END AS diagnostic_group 
		FROM diagnostic) d 
	LEFT JOIN mnt_vih_tb n USING(encounter_id)
	LEFT JOIN cohorte c ON d.patient_id = c.patient_id AND c.date_inclusion <= n.date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE current_date END >= n.date
	ORDER BY d.patient_id, c.encounter_id_inclusion, d.diagnostic_group, n.date),
-- The following CTEs create the active patient calculation by counting each day the cumulative admissions and exits from the cohort.
cohorte_active AS (
	SELECT
		c.date_inclusion, 
		c.date_de_sortie,
		dc.mnt, 
		dc.vih, 
		dc.tb, 
		dc.santé_mentale
	FROM cohorte c
	LEFT OUTER JOIN diagnostic_cohorte dc
		ON c.encounter_id_inclusion = dc.encounter_id_inclusion),
range_values AS (
	SELECT 
		date_trunc('day',min(ca.date_inclusion)) AS minval,
		date_trunc('day',max(CURRENT_DATE)) AS maxval
	FROM cohorte_active ca),
day_range AS (
	SELECT 
		generate_series(minval,maxval,'1 day'::interval) as day
	FROM range_values),   
daily_admissions AS (
	SELECT 
		date_trunc('day', ca.date_inclusion) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	GROUP BY 1),
daily_admissions_mnt AS (
	SELECT 
		date_trunc('day', ca.mnt) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE mnt IS NOT NULL
	GROUP BY 1),
daily_admissions_vih AS (
	SELECT 
		date_trunc('day', ca.vih) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE vih IS NOT NULL
	GROUP BY 1),
daily_admissions_tb AS (
	SELECT 
		date_trunc('day', ca.tb) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE tb IS NOT NULL
	GROUP BY 1),
daily_admissions_sm AS (
	SELECT 
		date_trunc('day', ca.santé_mentale) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE santé_mentale IS NOT NULL
	GROUP BY 1),
daily_exits AS (
	SELECT
		date_trunc('day',ca.date_de_sortie) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	GROUP BY 1),
daily_exits_mnt AS (
	SELECT
		date_trunc('day',ca.date_de_sortie) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE MNT IS NOT NULL
	GROUP BY 1),
daily_exits_vih AS (
	SELECT
		date_trunc('day',ca.date_de_sortie) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE vih IS NOT NULL
	GROUP BY 1),
daily_exits_tb AS (
	SELECT
		date_trunc('day',ca.date_de_sortie) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE tb IS NOT NULL
	GROUP BY 1),
daily_exits_sm AS (
	SELECT
		date_trunc('day',ca.date_de_sortie) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE santé_mentale IS NOT NULL
	GROUP BY 1),
daily_cohorte_active AS (
	SELECT 
		dr.day as reporting_day,
		SUM(da.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions,
		CASE
		    WHEN SUM(de.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE SUM(de.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits,  
		CASE
		    WHEN SUM(de.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN SUM(da.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (SUM(da.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				SUM(de.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS cohorte_active,
		SUM(dam.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_mnt,
		CASE
		    WHEN SUM(dem.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE SUM(dem.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_mnt,  
		CASE
		    WHEN SUM(dem.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN SUM(dam.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (SUM(dam.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				SUM(dem.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS cohorte_active_mnt,
		SUM(dav.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_vih,
		CASE
		    WHEN SUM(dev.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE SUM(dev.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_vih,  
		CASE
		    WHEN SUM(dev.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN SUM(dav.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (SUM(dav.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				SUM(dev.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS cohorte_active_vih,		
		SUM(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_tb,
		CASE
		    WHEN SUM(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE SUM(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_tb,  
		CASE
		    WHEN SUM(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN SUM(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (SUM(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				SUM(det.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS cohorte_active_tb,
		SUM(das.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_sm,
		CASE
		    WHEN SUM(des.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE SUM(des.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_sm,  
		CASE
		    WHEN SUM(des.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN SUM(das.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (SUM(das.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				SUM(des.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS cohorte_active_sm,	
		CASE 
			WHEN date(dr.day)::date = (date_trunc('MONTH', dr.day) + INTERVAL '1 MONTH - 1 day')::date THEN 1
		END AS last_day_of_month
	FROM day_range dr
	LEFT OUTER JOIN daily_admissions da ON dr.day = da.day
	LEFT OUTER JOIN daily_admissions_mnt dam ON dr.day = dam.day
	LEFT OUTER JOIN daily_admissions_vih dav ON dr.day = dav.day
	LEFT OUTER JOIN daily_admissions_tb dat ON dr.day = dat.day
	LEFT OUTER JOIN daily_admissions_sm das ON dr.day = das.day
	LEFT OUTER JOIN daily_exits de ON dr.day = de.day
	LEFT OUTER JOIN daily_exits_mnt dem ON dr.day = dem.day
	LEFT OUTER JOIN daily_exits_vih dev ON dr.day = dev.day
	LEFT OUTER JOIN daily_exits_tb det ON dr.day = det.day
	LEFT OUTER JOIN daily_exits_sm des ON dr.day = des.day)
-- Main query --
SELECT 
	dap.reporting_day::date,
	dap.cohorte_active,
	dap.cohorte_active_mnt AS mnt,
	dap.cohorte_active_vih AS vih,
	dap.cohorte_active_tb AS tb,
	dap.cohorte_active_sm AS santé_mentale
FROM daily_cohorte_active dap
WHERE dap.last_day_of_month = 1 and dap.reporting_day > date_trunc('MONTH', CURRENT_DATE) - INTERVAL '1 year'
GROUP BY dap.reporting_day, dap.cohorte_active, dap.cohorte_active_mnt, dap.cohorte_active_vih, dap.cohorte_active_tb, dap.cohorte_active_sm;