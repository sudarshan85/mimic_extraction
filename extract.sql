drop materialized view if exists cohort cascade;
create materialized view cohort as

select ie.subject_id, ie.hadm_id, ie.icustay_id

, pat.gender, pat.dob, pat.dod

-- , adm.admittime, adm.dischtime
, adm.admittime
-- , round((cast(extract(epoch from adm.dischtime - adm.admittime)/(60*60*24) as numeric)), 4) as los_hospital -- in days
, round((cast(extract(epoch from adm.admittime - pat.dob)/(60*60*24*365.242) as numeric)), 4) as admission_age -- in years
, adm.ethnicity, adm.admission_type, 
, 

