drop materialized view if exists co cascade;
create materialized view co as

-- icu level factors
select ie.subject_id, ie.hadm_id, ie.icustay_id, ie.intime, ie.outtime, ie.los

-- patient level factors
, pat.gender, pat.dob, pat.dod

-- hospital level factors
, adm.admittime, adm.dischtime
, round((cast(extract(epoch from adm.dischtime - adm.admittime)/(60*60*24) as numeric)), 4) as los_hospital -- in days
, round((cast(extract(epoch from adm.admittime - pat.dob)/(60*60*24*365.242) as numeric)), 4) as admission_age -- in years
, adm.deathtime, adm.admission_type, adm.admission_location, adm.discharge_location, adm.insurance
, adm.language, adm.religion, adm.marital_status, adm.ethnicity

-- wait time between hospital admission and icu intime
, round((cast(extract(epoch from ie.intime - adm.admittime)/(60*60) as numeric)), 4) as waiting -- in hours 

-- sequence of hospital admissions 
, dense_rank() over (partition by adm.subject_id order by admittime) as hospstay_seq

-- mark the first hospital stay
, case
  when dense_rank() over (partition by adm.subject_id order by adm.admittime) = 1 then true
  else false end as first_hosp_stay

-- sequence of icu admissions for current hospital admission
, dense_rank() over (partition by ie.hadm_id order by ie.intime) as icustay_seq

-- mark the first icu stay for current hospital admission
, case
  when dense_rank() over (partition by ie.hadm_id order by ie.intime) = 1 then true
  else false end as first_icu_stay

from icustays ie
inner join admissions adm
    on ie.hadm_id = adm.hadm_id
inner join patients pat
    on ie.subject_id = pat.subject_id
where adm.has_chartevents_data = 1
order by ie.subject_id, adm.admittime, ie.intime;
