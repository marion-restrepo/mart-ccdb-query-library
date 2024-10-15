-- This is a test file for the CBO register. 
-- adding a new line of code.
-- add some more code here as a demo
WITH sub-table1 AS (),
sub-table2 AS ()
-- Main query --
SELECT
*
FROM patient_identifier
LEFT OUTER JOIN sub-table1 st1 USING(patient_id)
LEFT OUTER JOIN sub-table2 st2 USING(patient_id)