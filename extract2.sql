drop materialized view if exists co2 cascade;
create materialized view co2 as

with 
patadm as (
  select p.subject_id, p.gender, p.dob, p.dod, adm.hadm_id, adm.admittime, adm.dischtime,
  adm.deathtime, adm.admission_type, adm.insurance, adm.language, adm.religion, adm.marital_status,
  adm.ethnicity, adm.diagnosis, adm.has_chartevents_data,
  -- in years
  round((cast(extract(epoch from adm.admittime - p.dob)/(60*60*24*365.242) as numeric)), 2) as
  admission_age,
  -- in days
  round((cast(extract(epoch from adm.dischtime - adm.admittime)/(60*60*24) as numeric)), 2) as
  los_hospital,
  -- mark the first hospital stay
  case
    when dense_rank() over (partition by p.subject_id order by adm.admittime) = 1 then true
  -- include in first_hosp_stay if its been atleast a month since prvious admission. Using lag() as
  -- shown here: http://bit.ly/2KpJaeg
    when round((cast(extract(epoch from adm.admittime - lag(adm.admittime, 1) over (partition by
      p.subject_id order by adm.admittime) )/(60*60*30) as numeric)), 2) > 30.0 then true
    else false end as first_hosp_stay
  from patients p
  inner join admissions adm
    on adm.subject_id = p.subject_id
)
select pa.subject_id, pa.gender, pa.dob, pa.dod, pa.hadm_id, pa.admittime, pa.dischtime,
pa.deathtime, pa.admission_type, pa.insurance, pa.language, pa.religion, pa.marital_status,
pa.ethnicity, pa.diagnosis, pa.admission_age, pa.los_hospital, pa.first_hosp_stay, icu.icustay_id,
icu.intime, icu.outtime, icu.los,
-- wait time between hospital admission and icu intime in hours
-- mark the first icu stay for current hospital admission
case
  when dense_rank() over (partition by pa.hadm_id order by icu.intime) = 1 then true
  else false end as first_icu_stay,
round((cast(extract(epoch from icu.intime - pa.admittime)/(60*60) as numeric)), 2) as wait_time
from patadm pa
inner join icustays icu
  on icu.hadm_id = pa.hadm_id
where pa.has_chartevents_data = 1
order by pa.subject_id, pa.admittime, icu.intime;
