drop materialized view if exists co cascade;
create materialized view co as

-- patient level factors
select pat.subject_id, pat.gender, pat.dob, pat.dod

-- hospital level factors
, adm.hadm_id, adm.admittime, adm.dischtime, adm.deathtime, adm.admission_type
, adm.insurance, adm.language, adm.religion, adm.marital_status, adm.ethnicity
, adm.diagnosis

-- icu level factors
, icu.icustay_id, icu.intime, icu.outtime, icu.los

-- noteevents
, ne.charttime, ne.storetime, ne.category, ne.description, ne.text

-- in hours 
, round((cast(extract(epoch from ne.storetime - adm.admittime)/(60*60) as numeric)), 2) as write

, case
  when cast(extract(epoch from ne.storetime - adm.admittime)/(60*60) as numeric) < cast(extract(epoch from icu.intime - adm.admittime)/(60*60) as numeric) then true
  else false end as note_flag

-- in years
, round((cast(extract(epoch from adm.admittime - pat.dob)/(60*60*24*365.242) as numeric)), 2) as
admission_age
-- in days
, round((cast(extract(epoch from adm.dischtime - adm.admittime)/(60*60*24) as numeric)), 2) as
los_hospital

-- wait time between hospital admission and icu intime in hours
, round((cast(extract(epoch from icu.intime - adm.admittime)/(60*60) as numeric)), 2) as wait_time

-- mark the first hospital stay
, case
  when dense_rank() over (partition by pat.subject_id order by adm.admittime) = 1 then true
-- include in first_hosp_stay if its been atleast a month since prvious admission. Using lag() as
-- shown here: http://bit.ly/2KpJaeg
  when round((cast(extract(epoch from adm.admittime - lag(adm.admittime, 1) over (partition by
    pat.subject_id order by adm.admittime) )/(60*60*30) as numeric)), 2) > 30.0 then true
  else false end as first_hosp_stay

-- mark the first icu stay for current hospital admission
, case
  when dense_rank() over (partition by adm.hadm_id order by icu.intime) = 1 then true
  else false end as first_icu_stay

from icustays icu
inner join admissions adm
    on icu.hadm_id = adm.hadm_id
inner join patients pat
    on adm.subject_id = pat.subject_id
inner join proxy_ne ne
    on ne.hadm_id = adm.hadm_id
where adm.has_chartevents_data = 1
and ne.iserror is null
order by pat.subject_id, adm.admittime, icu.intime;

-- sequence of hospital admissions 
-- , dense_rank() over (partition by pat.subject_id order by adm.admittime) as hospstay_seq
-- sequence of icu admissions for current hospital admission
-- , dense_rank() over (partition by adm.hadm_id order by icu.intime) as icustay_seq

-- , lag(adm.admittime, 1) over (partition by pat.subject_id order by adm.admittime) as prev
-- , round((cast(extract(epoch from adm.admittime - lag(adm.admittime, 1) over (partition by pat.subject_id order by adm.admittime) )/(60*60*30) as numeric)), 2) as diff -- in months

-- and round((cast(extract(epoch from icu.intime - adm.admittime)/(60*60) as numeric)), 4)  > 0.0
-- icu level factors
-- select ie.subject_id, ie.hadm_id, ie.icustay_id, ie.intime, ie.outtime, ie.los

-- -- patient level factors
-- , pat.gender, pat.dob, pat.dod

-- -- hospital level factors
-- , adm.admittime, adm.dischtime
-- , round((cast(extract(epoch from adm.dischtime - adm.admittime)/(60*60*24) as numeric)), 4) as los_hospital -- in days
-- , round((cast(extract(epoch from adm.admittime - pat.dob)/(60*60*24*365.242) as numeric)), 4) as admission_age -- in years
-- , adm.deathtime, adm.admission_type, adm.admission_location, adm.discharge_location, adm.insurance
-- , adm.language, adm.religion, adm.marital_status, adm.ethnicity

-- -- wait time between hospital admission and icu intime
-- , round((cast(extract(epoch from ie.intime - adm.admittime)/(60*60) as numeric)), 4) as waiting -- in hours 

-- -- sequence of hospital admissions 
-- , dense_rank() over (partition by adm.subject_id order by admittime) as hospstay_seq

-- -- mark the first hospital stay
-- , case
  -- when dense_rank() over (partition by adm.subject_id order by adm.admittime) = 1 then true
  -- else false end as first_hosp_stay

-- -- sequence of icu admissions for current hospital admission
-- , dense_rank() over (partition by ie.hadm_id order by ie.intime) as icustay_seq

-- -- mark the first icu stay for current hospital admission
-- , case
  -- when dense_rank() over (partition by ie.hadm_id order by ie.intime) = 1 then true
  -- else false end as first_icu_stay

-- from icustays ie
-- inner join admissions adm
    -- on ie.hadm_id = adm.hadm_id
-- inner join patients pat
    -- on ie.subject_id = pat.subject_id
-- where adm.has_chartevents_data = 1
-- order by ie.subject_id, adm.admittime, ie.intime;
