-- drop materialized view if exists patadm cascade;
-- create materialized view patadm as

-- select pat.subject_id
-- , pat.dob, adm.hadm_id, adm.admittime
-- -- age at admission in years
-- , round((cast(extract(epoch from adm.admittime - pat.dob)/(60*60*24*365.242) as numeric)), 2) as
-- admission_age

-- from patients pat
-- inner join admissions adm
  -- on adm.subject_id = pat.subject_id
-- where adm.has_chartevents_data = 1
-- order by pat.subject_id, adm.admittime;

-- drop materialized view if exists admicu cascade;
-- create materialized view admicu as

-- select adm.hadm_id
-- , adm.admittime
-- , icu.icustay_id, icu.intime

-- -- time period between hospital admission and its 1st icu visit in hours
-- , round((cast(extract(epoch from icu.intime - adm.admittime)/(60*60) as numeric)), 2) as wait_period

-- , case
-- -- mark the first hospital adm 
    -- when dense_rank() over (partition by adm.subject_id order by adm.admittime) = 1 then true
-- -- mark subsequent hospital adms if its been atleast a month since previous admission.
-- -- Defined using lag() as shown here: http://bit.ly/2KpJaeg
    -- when round((cast(extract(epoch from adm.admittime - lag(adm.admittime, 1) over (partition by
      -- adm.subject_id order by adm.admittime))/(60*60*24) as numeric)), 2) > 30.0 then true
    -- else false end as include_adm

-- -- mark the first icu stay for current hospital admission
-- , case
    -- when dense_rank() over (partition by icu.hadm_id order by icu.intime) = 1 then true
    -- else false end as include_icu

-- from admissions adm
-- inner join icustays icu
  -- on icu.hadm_id = adm.hadm_id;

-- drop materialized view if exists admne cascade;
-- create materialized view admne as

-- select adm.hadm_id
-- , adm.admittime
-- , ne.charttime as ne_charttime

-- -- time period between hospital admission and the note charttime in hours
-- , round((cast(extract(epoch from ne.charttime - adm.admittime)/(60*60) as numeric)), 2) as
-- ne_adm_period 

-- from admissions adm
-- inner join noteevents ne
  -- on ne.hadm_id = adm.hadm_id;

-- drop materialized view if exists admech cascade;
-- create materialized view admech as

-- select adm.hadm_id
-- , adm.admittime
-- , ech.charttime as ech_charttime

-- -- time period between echo hospital admission and echo charttime
-- , round((cast(extract(epoch from ech.charttime - adm.admittime)/(60*60) as numeric)), 2) as
-- ech_adm_period

-- from admissions adm
-- inner join echodata ech
  -- on ech.hadm_id = adm.hadm_id;

drop materialized view if exists co cascade;
create materialized view co as

select pa.subject_id
, pa.dob, pa.hadm_id, pa.admittime, pa.admission_age
, ai.icustay_id, ai.intime, ai.wait_period, ai.include_adm, ai.include_icu
, an.ne_charttime, an.ne_adm_period
, ae.ech_charttime, ae.ech_adm_period

-- time period between note charttime and first icu visit in hours
, round((cast(extract(epoch from ai.intime - an.ne_charttime)/(60*60) as numeric)), 2) as
icu_ne_period

-- time period between echo hospital admission and echo charttime
, round((cast(extract(epoch from ai.intime - ae.ech_charttime)/(60*60) as numeric)), 2) as
icu_ech_period

from patadm pa
inner join admicu ai
  on ai.hadm_id = pa.hadm_id
inner join admne an
  on an.hadm_id = pa.hadm_id
inner join admech ae
  on ae.hadm_id = pa.hadm_id
order by pa.subject_id, pa.admittime;

