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
dernière_diagnostic_cohorte_pivot AS (
	SELECT 
		DISTINCT ON (encounter_id_inclusion, patient_id, date) encounter_id_inclusion, 
		patient_id, 
		date,
		MAX (CASE WHEN diagnostic IN ('Asthme','Drépanocytose','Insuffisance renale chronique','Maladie cardiovasculaire','Bronchopneumopathie chronique obstructive','Diabète sucré de type 1','Diabète sucré de type 2','Hypertension','Hypothyroïdie','Hyperthyroïdie','Épilepsie focale','Épilepsie généralisée','Épilepsie non classifiée','Autre') THEN 1 ELSE NULL END) AS mnt,
		MAX (CASE WHEN diagnostic IN ('Tuberculose pulmonaire','Tuberculose extrapulmonaire') THEN 1 ELSE NULL END) AS tb,
		MAX (CASE WHEN diagnostic = 'Infection par le VIH' THEN 1 ELSE NULL END) AS vih,
		MAX (CASE WHEN diagnostic = 'Troubles de santé mentale' THEN 1 ELSE NULL END) AS santé_mentale	
	FROM dernière_diagnostic_cohorte
	GROUP BY encounter_id_inclusion, patient_id, date),
-- The following CTEs create the active patient calculation by counting each day the cumulative admissions and exits from the cohort.
cohorte_active AS (
	SELECT
		c.date_inclusion, 
		c.date_de_sortie,
		CASE 
			WHEN ddcp.mnt = 1 AND ddcp.tb IS NULL AND ddcp.vih IS NULL AND ddcp.santé_mentale IS NULL THEN 'mnt'
			WHEN ddcp.mnt IS NULL AND ddcp.tb = 1 AND ddcp.vih IS NULL AND ddcp.santé_mentale IS NULL THEN 'tb'
			WHEN ddcp.mnt IS NULL AND ddcp.tb IS NULL AND ddcp.vih = 1 AND ddcp.santé_mentale IS NULL THEN 'vih'
			WHEN ddcp.mnt IS NULL AND ddcp.tb IS NULL AND ddcp.vih IS NULL AND ddcp.santé_mentale = 1 THEN 'santé_mentale'
			WHEN ddcp.mnt = 1 AND ddcp.tb IS NULL AND ddcp.vih = 1 AND ddcp.santé_mentale IS NULL THEN 'mnt_vih' ELSE NULL END AS cohorte
	FROM cohorte c
	LEFT OUTER JOIN dernière_diagnostic_cohorte_pivot ddcp
		ON c.encounter_id_inclusion = ddcp.encounter_id_inclusion),
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
		date_trunc('day', ca.date_inclusion) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE cohorte = 'mnt'
	GROUP BY 1),
daily_admissions_vih AS (
	SELECT 
		date_trunc('day', ca.date_inclusion) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE cohorte = 'vih'
	GROUP BY 1),
daily_admissions_tb AS (
	SELECT 
		date_trunc('day', ca.date_inclusion) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE cohorte = 'tb'
	GROUP BY 1),
daily_admissions_sm AS (
	SELECT 
		date_trunc('day', ca.date_inclusion) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE cohorte = 'santé_mentale'
	GROUP BY 1),
daily_admissions_mnt_vih AS (
	SELECT 
		date_trunc('day', ca.date_inclusion) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE cohorte = 'mnt_vih'
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
	WHERE cohorte = 'mnt'
	GROUP BY 1),
daily_exits_vih AS (
	SELECT
		date_trunc('day',ca.date_de_sortie) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE cohorte = 'vih'
	GROUP BY 1),
daily_exits_tb AS (
	SELECT
		date_trunc('day',ca.date_de_sortie) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE cohorte = 'tb'
	GROUP BY 1),
daily_exits_sm AS (
	SELECT
		date_trunc('day',ca.date_de_sortie) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE cohorte = 'santé_mentale'
	GROUP BY 1),
daily_exits_mnt_vih AS (
	SELECT
		date_trunc('day',ca.date_de_sortie) AS day,
		count(*) AS patients
	FROM cohorte_active ca
	WHERE cohorte = 'mnt_vih'
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
		SUM(damv.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_mnt_vih,
		CASE
		    WHEN SUM(demv.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE SUM(demv.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_mnt_vih,  
		CASE
		    WHEN SUM(demv.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN SUM(damv.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (SUM(damv.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				SUM(demv.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS cohorte_active_mnt_vih,		
		CASE 
			WHEN date(dr.day)::date = (date_trunc('MONTH', dr.day) + INTERVAL '1 MONTH - 1 day')::date THEN 1
		END AS last_day_of_month
	FROM day_range dr
	LEFT OUTER JOIN daily_admissions da ON dr.day = da.day
	LEFT OUTER JOIN daily_admissions_mnt dam ON dr.day = dam.day
	LEFT OUTER JOIN daily_admissions_vih dav ON dr.day = dav.day
	LEFT OUTER JOIN daily_admissions_tb dat ON dr.day = dat.day
	LEFT OUTER JOIN daily_admissions_sm das ON dr.day = das.day
	LEFT OUTER JOIN daily_admissions_mnt_vih damv ON dr.day = damv.day
	LEFT OUTER JOIN daily_exits de ON dr.day = de.day
	LEFT OUTER JOIN daily_exits_mnt dem ON dr.day = dem.day
	LEFT OUTER JOIN daily_exits_vih dev ON dr.day = dev.day
	LEFT OUTER JOIN daily_exits_tb det ON dr.day = det.day
	LEFT OUTER JOIN daily_exits_sm des ON dr.day = des.day
	LEFT OUTER JOIN daily_exits_mnt_vih demv ON dr.day = demv.day)
-- Main query --
SELECT 
	dap.reporting_day::date,
	dap.cohorte_active,
	dap.cohorte_active_mnt AS mnt,
	dap.cohorte_active_vih AS vih,
	dap.cohorte_active_tb AS tb,
	dap.cohorte_active_sm AS santé_mentale,
	dap.cohorte_active_mnt_vih AS mnt_vih
FROM daily_cohorte_active dap
WHERE dap.last_day_of_month = 1 and dap.reporting_day > date_trunc('MONTH', CURRENT_DATE) - INTERVAL '1 year'
GROUP BY dap.reporting_day, dap.cohorte_active, dap.cohorte_active_mnt, dap.cohorte_active_vih, dap.cohorte_active_tb, dap.cohorte_active_sm, dap.cohorte_active_mnt_vih;