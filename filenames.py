#!/usr/bin/env python

from socket import gethostname
from pathlib import Path
from argparse import Namespace

mimic_path = Path('./data/mimic3/gz')
eicu_path = Path('./data/eicu/gz')
ext = '.csv.gz'

mimic_files = Namespace(
    path=mimic_path,
    admissions=mimic_path/f'ADMISSIONS{ext}',
    callout=mimic_path/f'CALLOUT{ext}',
    caregivers=mimic_path/f'CAREGIVERS{ext}',
    chart=mimic_path/f'CHARTEVENTS{ext}',
    cpt=mimic_path/f'CPTEVENTS{ext}',
    datetime=mimic_path/f'DATETIMEEVENTS{ext}',
    diagnoses_icd=mimic_path/f'DIAGNOSES_ICD{ext}',
    d_icd_diagnoses=mimic_path/f'D_ICD_DIAGNOSES{ext}',
    d_icd_procedures=mimic_path/f'D_ICD_PROCEDURES{ext}',
    d_items=mimic_path/f'D_ITEMS{ext}',
    d_labitems=mimic_path/f'D_LABITEMS{ext}',
    drgcodes=mimic_path/f'DRGCODES{ext}',
    icustays=mimic_path/f'ICUSTAYS{ext}',
    input_cv=mimic_path/f'INPUTEVENTS_CV{ext}',
    input_mv=mimic_path/f'INPUTEVENTS_MV{ext}',
    lab=mimic_path/f'LABEVENTS{ext}',
    microbiology=mimic_path/f'MICROBIOLOGYEVENTS{ext}',
    note=mimic_path/f'NOTEEVENTS{ext}',
    output=mimic_path/f'OUTPUTEVENTS{ext}',
    patients=mimic_path/f'PATIENTS{ext}',
    procedures_mv=mimic_path/f'PROCEDUREEVENTS_MV{ext}',
    procedures_icd=mimic_path/f'PROCEDURES_ICD{ext}',
    services=mimic_path/f'SERVICES{ext}',
    transfers=mimic_path/f'TRANSFERS{ext}',
    )

eicu_files = Namespace(
    path=eicu_path,
    admissiondrug=eicu_path/f'admissionDrug{ext}',
    admissiondx=eicu_path/f'admissionDx{ext}',
    allergy=eicu_path/f'allergy{ext}',
    apache_aps=eicu_path/f'apacheApsVar{ext}',
    apache_patient_result=eicu_path/f'apachePatientResult{ext}',
    apache_pred=eicu_path/f'apachePredVar{ext}',
    careplan=eicu_path/f'carePlanCareProvider{ext}',
    careplan_eol=eicu_path/f'carePlanEOL{ext}',
    careplan_gen=eicu_path/f'carePlanGeneral{ext}',
    careplan_goal=eicu_path/f'carePlanGoal{ext}',
    careplan_id=eicu_path/f'carePlanInfectiousDisease{ext}',
    customlab=eicu_path/f'customLab{ext}',
    diagnosis=eicu_path/f'diagnosis{ext}',
    hospital=eicu_path/f'hospital{ext}',
    infusion_drug=eicu_path/f'infusionDrug{ext}',
    intake_output=eicu_path/f'intakeOutput{ext}',
    lab=eicu_path/f'lab{ext}',
    medication=eicu_path/f'medication{ext}',
    microlab=eicu_path/f'microLab{ext}',
    note=eicu_path/f'note{ext}',
    nurse_assessment=eicu_path/f'nurseAssessment{ext}',
    nurse_care=eicu_path/f'nurseCare{ext}',
    nurse_chart=eicu_path/f'nurseCharting{ext}',
    past_history=eicu_path/f'pastHistory{ext}',
    patient=eicu_path/f'patient{ext}',
    physical_exam=eicu_path/f'physicalExam{ext}',
    respiratory_care=eicu_path/f'respiratoryCare{ext}',
    respiratory_chart=eicu_path/f'respiratoryCharting{ext}',
    treatment=eicu_path/f'treatment{ext}',
    vital_aperiodic=eicu_path/f'vitalAperiodic{ext}',
    vital_periodic=eicu_path/f'vitalPeriodic{ext}',
    )
