drop materialized view if exists co cascade;
create materialized view co as

select
-- patient level factors
pat.subject_id, pat.dob,
-- hospital admission level factors
adm.hadm_id, adm.admittime,
-- icu level factors
icu.icustay_id, icu.intime,

-- age at admission in years
round((cast(extract(epoch from adm.admittime - pat.dob)/(60*60*24*365.242) as numeric)), 2) as
admission_age,

case
-- mark the first hospital adm 
  when dense_rank() over (partition by pat.subject_id order by adm.admittime) = 1 then true
-- mark subsequent hospital adms if its been atleast a month since previous admission.
-- Defined using lag() as shown here: http://bit.ly/2KpJaeg
  when round((cast(extract(epoch from adm.admittime - lag(adm.admittime, 1) over (partition by
    pat.subject_id order by adm.admittime))/(60*60*24) as numeric)), 2) > 30.0 then true
  else false end as include_adm,

-- mark the first icu stay for current hospital admission
case
  when dense_rank() over (partition by adm.hadm_id order by icu.intime) = 1 then true
  else false end as include_icu,

-- time period between hospital admission and its 1st icu visit in hours
round((cast(extract(epoch from icu.intime - adm.admittime)/(60*60) as numeric)), 2) as wait_period

from patients pat
inner join admissions adm
  on adm.subject_id = pat.subject_id
inner join icustays icu
  on icu.hadm_id = adm.hadm_id
where adm.has_chartevents_data = 1
-- discard records which as icu intime earlier than hospital admission time
and round((cast(extract(epoch from icu.intime - adm.admittime)/(60*60) as numeric)), 2) > 0.0
order by pat.subject_id, adm.admittime, icu.intime;

-- these were used for creating query for marking hospital adms
-- lag(adm.admittime, 1) over (partition by pat.subject_id order by adm.admittime) as prev,
-- dense_rank() over (partition by pat.subject_id order by adm.admittime) as adm_seq,
-- case
  -- when dense_rank() over (partition by pat.subject_id order by adm.admittime) = 1 then true
  -- else false end as include_adm1,
-- round((cast(extract(epoch from adm.admittime - lag(adm.admittime, 1) over (partition by pat.subject_id order by adm.admittime))/(60*60*24) as numeric)), 2) as adm_delta,

-- this was used for creating query for marking 1st icu stay
-- dense_rank() over (partition by adm.hadm_id order by icu.intime) as icu_seq,
