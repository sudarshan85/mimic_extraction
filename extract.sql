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

lag(adm.admittime, 1) over (partition by pat.subject_id order by adm.admittime) as prev,

dense_rank() over (partition by pat.subject_id order by adm.admittime) as adm_seq,

-- mark the first hospital stay
case when dense_rank() over (partition by pat.subject_id order by adm.admittime) = 1 then true else
  false end as include_adm1,

round((cast(extract(epoch from adm.admittime - lag(adm.admittime, 1) over (partition by pat.subject_id order by adm.admittime))/(60*60*24) as numeric)), 2) as adm_delta,

case when round((cast(extract(epoch from adm.admittime - lag(adm.admittime, 1) over (partition by
  pat.subject_id order by adm.admittime))/(60*60*24) as numeric)), 2) > 30.0 then true
  when dense_rank() over (partition by pat.subject_id order by adm.admittime) = 1 then true
  else false end as include_adm2

from patients pat
inner join admissions adm
  on adm.subject_id = pat.subject_id
inner join icustays icu
  on icu.hadm_id = adm.hadm_id
order by pat.subject_id, adm.admittime, icu.intime;
