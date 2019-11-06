-- ------------------------------------------------------------------
-- MIMIC version: MIMIC-III v1.4
-- ------------------------------------------------------------------
-- NOTE: I made sure that the hadm_ids produced by the query are all the
-- same hadm_ids in the original notes_df produced after prepping

drop materialized view if exists notes_all_co cascade;
create materialized view notes_all_co as

with inter as
(
  select adm.hadm_id
  , adm.admittime
  , adm.admission_type
  , adm.ethnicity
  , pat.subject_id
  , pat.dob
  , pat.gender
  , pat.expire_flag

  , round((cast(extract(epoch from adm.admittime - pat.dob)/(60*60*24*365.242) as numeric)), 2) as
  admission_age

  from admissions adm
  inner join notes_all_adms ca on adm.hadm_id = ca.hadm_id 
  inner join patients pat on pat.subject_id = adm.subject_id
)

select hadm_id
, subject_id
, admission_type
, ethnicity
, admission_age
, gender
, expire_flag

from inter
order by hadm_id;
