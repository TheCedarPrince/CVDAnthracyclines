using DrWatson
@quickactivate "CVDAnthracyclines"
using JSON3,Revise
using HTTP
using OMOPCDMCohortCreator
using CSV
using DataFrames
using DBInterface
import DBInterface:
    connect,
    execute
import LibPQ:
    Connection
using FunSQL
using FunSQL: From,Limit,render,Get,Select,Join,LeftJoin,Agg,Group,Order,Asc,Desc


conn = connect(
    Connection, 
    "host=localhost port=5432 dbname=synthea user=thecedarprince password=JJJOjjjo1*"
)    
GenerateDatabaseDetails(:postgresql, "omop")
tables = GenerateTables(conn, inplace = false, exported = true)
person = tables[:person]
procedure_occurrence = tables[:procedure_occurrence]
concept_ancestor= tables[:concept_ancestor]
concept= tables[:concept]
vocabulary= tables[:vocabulary]
condition_occurrence= tables[:condition_occurrence]


"""
we created manually a DataFrame where in given column of name "col_name" 
we have put manually 1's if we accept the code and Id of the concept is "Id"
dataframe is based on csv downloaded from atlas query and modified manually
"""
function get_manually_accepted_from_df(csv_path,col_name,conn,condition_occurrence)
    miocard_df = CSV.File(csv_path) |> DataFrame
    df=miocard_df[!,["Id","Name",col_name,"Vocabulary"]]
    df = df[.!ismissing.(df[!,col_name]), :]
    df = df[.!ismissing.(df.Id), :]
    df_was = df[df[!,col_name] .== 1, :]
    # unique(df_was[!,"was"])
    # df_was[!,"Id"]
    all_miocardial_infarction_ids=ConditionFilterPersonIDs(df_was[!,"Id"], conn;tab = condition_occurrence)
    return all_miocardial_infarction_ids
end   




miocard_path="/home/jakubmitura/projects/CVDAnthracyclines/data/ATLAS_miocardial_infarction.csv"
col_name="was"
mio_infarct_patient_ids=get_manually_accepted_from_df(miocard_path,col_name,conn,condition_occurrence)[!,"person_id"]


#translation of code to standard vocabulary
"""
table source_to_standard_vocab_map
we look for row with value of a query in "source_concept_id" column
    then print result from column_names
    ["target_concept_id", "target_concept_name","target_vocabulary_id","target_domain_id" ]
additionaly we can get vocabulary name using 
vocabulary table and column_name "vocabulary_name" in row where "vocabulary_id" fit

"""
## get concept name
"""
table concept
we look for row with value of a query in "concept_id" column
    then print result from column_names
    ["concept_name","concept_code","concept_class_id"]
"""
function get_concept_name(conn, concept_id)
    sqq=""" SELECT concept_name, concept_code, concept_class_id
    FROM omop.concept
    WHERE concept_id = $(concept_id)
    Limit 1 ;"""
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_concept_name

get_concept_name(conn, mio_infarct_concept_ids[11])

## get concept name
"""
table concept_class
we look for row with value of a query in "concept_class_id" column
    then print result from column_name "concept_class_name"
"""
function get_concept_class_name(conn, concept_class_id)
    sqq=""" SELECT concept_class_name,concept_class_id
    FROM omop.concept_class
    WHERE concept_class_id = $(concept_class_id)
    Limit 1 ;"""
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_concept_class_name



## get concept relation for mapping
""" 
table concept_relationship
we look for row with value of a query in "concept_id_1" column
    then print result from column_names
    ["concept_id_2","relationship_id"]
"""

## get info about death
"""
table death
we look for row with value of a query in "person_id" column
    then print result from column_names
    ["death_date","cause_concept_id"]
"""

## get info about observation
""" 
table observation
we look for row with value of a query in "person_id" column
    then print result from column_names
    ["observation_date","observation_type_concept_id","value_as_number","visit_occurrence_id","observation_event_id"]
"""
function get_observation(conn, person_id)
    # sqq=""" SELECT observation_date, observation_type_concept_id, value_as_number, visit_occurrence_id, observation_event_id
    # FROM omop.observation
    # WHERE person_id = $(person_id);
    # """

    sqq="""
    SELECT o.observation_date, c.concept_name, o.value_as_number, o.visit_occurrence_id, o.observation_event_id
    FROM omop.observation o
    JOIN omop.concept c ON o.observation_type_concept_id = c.concept_id
    WHERE o.person_id = $(person_id);"""
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_observation

get_observation(conn, mio_infarct_patient_ids[1])


## get info about device exposure (maybe radiotheraphy)
"""
table device_exposure
we look for row with value of a query in "person_id" column
    then print result from column_names
    ["device_exposure_start_date","device_exposure_end_date","device_concept_id","visit_occurrence_id"]
"""


## get info about visit of a patient
"""
table visit_detail
look for patient of id in column "person_id" and visit id in column "visit_occurrence_id"
then we want to print when it was in column "visit_start_date" and "visit_end_date"
"""

## get more info about visit of a patient
"""
table visit_occurrence
look for patient of id in column "person_id" and visit id in column "visit_occurrence_id"
then we want to print when it was in column "visit_start_date" and "visit_end_date"
"""

## get dose of the drug
"""
table dose_era
we look for a dose for a given drug ogf given person in given time
so we look for a row with queried patient id from column "person_id"   
in dose era "dose_era_id"   we want to also print concept name on the basis of
"drug_concept_id" column in the end we want dose value from column "dose_value"
"""

## get info when drug was given in given dose
"""
table dose_era
we look for patient id in column "person_id" and drug id in column "drug_concept_id"
then we want to print when it was given so we look for "dose_era_start_date" and "dose_era_end_date"
"""

## get info when drug was given irrespective of dose
"""
table drug_era
we look for patient id in column "person_id" and drug id in column "drug_concept_id"
then we want to print when it was given so we look for "drug_era_start_date" and "drug_era_end_date"
"""


## get into about conditions 
"""
table condition_occurrence
we look for patient id in column "person_id" and condition id in column "condition_concept_id"
then we want to print when it was given so we look for "condition_start_date" and "condition_end_date"
"""
function get_condition_occurrence(conn, person_id)
    sqq=""" SELECT condition_start_date, condition_end_date
    FROM omop.condition_occurrence
    WHERE person_id = $(person_id);
    """
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_condition_occurrence



## get demographic info about patient
"""
table person
we look for patient id in column "person_id" and we want to print
    columns["gender_concept_id","year_of_birth" ]
"""


"""
we want to select single patient in the selected cohort and single visit of this patient
and display in human readle form all information about this patient including drugs,procedure, events and so on 
"""
#total observation time
"""
table observation_period
get the total observation period of the patient by looking for a row with patient_id in column "person_id" 
then subtract value of the date from column "observation_period_end_date" nad column  and  "observation_period_start_date"
"""

#measurements
"""
table measurement
we can get measurements of the patient by looking for a row with patient_id in column "person_id"
    then when it was in column "measurement_date" what kind of measurement based on column "measurement_type_concept_id" 
    value "value_as_number" and ranges so is it in range also based on column_names
    ["range_low","range_high"]
"""


#procedure
"""
table procedure_occurrence
we can get procedures of the patient by looking for a row with patient_id in column "person_id"
    then when it was in column "procedure_date" what kind of procedure based on column "procedure_type_concept_id" 
    value and ranges so is it in range also based on column_names
    ["procedure_source_value","procedure_source_concept_id"]
"""


get_ancestors(conn, 192671)

sqq=""" SELECT C.concept_id as ancestor_concept_id, C.concept_name as ancestor_concept_name, VA.vocabulary_name, A.min_levels_of_separation, A.max_levels_of_separation
FROM omop.concept_ancestor A, omop.concept C, omop.vocabulary VA
WHERE A.ancestor_concept_id = C.concept_id
AND C.vocabulary_id = VA.vocabulary_id
AND A.descendant_concept_id = 192671
AND A.ancestor_concept_id<>A.descendant_concept_id
Limit 20 ;"""


df=DBInterface.execute(conn, sqq) |> DataFrame


"""
get concept id like for example procedure_type_concept_id then look for its ancestor in 
concept_ancestor table aggregate to check what ancestor concepts are most common
"""
sq=From(concept_ancestor) |>
Join(:ancestor_concept_id => From(procedure_occurrence),
Get.concept_ancestor.ancestor_concept_id .== Get.procedure_occurrence.procedure_type_concept_id, left=true) |>
Group(Get.ancestor_concept_id) |>
Select(Get.ancestor_concept_id,Agg.count()) |>  
Order(Get.count |> Desc())|> 
q -> render(q, dialect=OMOPCDMCohortCreator.dialect)
df=DBInterface.execute(conn, sq) |> DataFrame





From(concept_ancestor) |>
Join(:ancestor_concept_id => From(procedure_occurrence),
Get.concept_ancestor.ancestor_concept_id .==Get.procedure_occurrence.procedure_occurrence_id) |>
q -> render(q, dialect=OMOPCDMCohortCreator.dialect)


sq=From(concept_ancestor) |>
Join(:ancestor_concept_id => From(procedure_occurrence),
Get.concept_ancestor.ancestor_concept_id .==Get.procedure_occurrence.procedure_occurrence_id) |>
Limit(2) |>
q -> render(q, dialect=OMOPCDMCohortCreator.dialect)
df=DBInterface.execute(conn, sq) |> DataFrame


# |>
# Select(Get.procedure_occurrence_id, Get.ancestor_concept_id)

procedure_occurrence.procedure_occurrence_id concept_ancestor.ancestor_concept_id

from(concept_ancestor, as=:A)
join(concept, as=:C, on=:ancestor_concept_id => :concept_id)
join(vocabulary, as=:VA, on=:C => :vocabulary_id => :VA => :vocabulary_id)



select(
    ancestor_concept_id = concept_id,
    ancestor_concept_name = concept_name,
    ancestor_concept_code = concept_code,
    ancestor_concept_class_id = concept_class_id,
    vocabulary_id,
    vocabulary_name = vocabulary_name,
    min_levels_of_separation,
    max_levels_of_separation
)
from(concept_ancestor, as=:A)
join(concept, as=:C, on=:ancestor_concept_id => :concept_id)
join(vocabulary, as=:VA, on=:C => :vocabulary_id => :VA => :vocabulary_id)
filter(
    :A => :ancestor_concept_id != :A => :descendant_concept_id,
    :A => :descendant_concept_id == 192671,
    Fun.between(sysdate(), :valid_start_date, :valid_end_date)
)
order(:vocabulary_id, :min_levels_of_separation)

"""
find ancestors of given concept based on https://github.com/OHDSI/OMOP-Queries/blob/master/md/General.md
"""
function get_ancestors()

end # get_ancestors  












[[1,2],[1,3]]+[[1,2],[1,2]]










sql = From(person) |> Select(Get.person_id) |> x -> render(x, dialect=OMOPCDMCohortCreator.dialect)
DBInterface.execute(conn, sql) |> DataFrame



q = From(procedure_occurrence)|>
Select(Get.procedure_concept_id) |>
q -> render(q, dialect=OMOPCDMCohortCreator.dialect)
all_procedure_ids=DBInterface.execute(conn, q) |> DataFrame
all_procedure_ids=unique(all_procedure_ids.procedure_concept_id)


sample_id=all_procedure_ids[1]

path = "https://atlas-demo.ohdsi.org/WebAPI/vocabulary/ATLASPROD/concept/$(sample_id)/ancestors/"
ancestors = 
    HTTP.get(path) |> 
    x -> String(x.body) |> 
    JSON3.read

  


now i want to get back to ids of patiants with miocardial Infarction
look through their procedures drugs events diseases in each case look 1 level up
in hierarchy and print 20 most common procedures drugs events diseases





q = From(procedure_occurrence)|>
Limit(1) |>
q -> render(q, dialect=OMOPCDMCohortCreator.dialect)
df=DBInterface.execute(conn, q) |> DataFrame
"""
 "procedure_occurrence_id"
 "person_id"
 "procedure_concept_id"
 "procedure_date"
 "procedure_datetime"
 "procedure_end_date"
 "procedure_end_datetime"
 "procedure_type_concept_id"
 "modifier_concept_id"
 "quantity"
 "provider_id"
 "visit_occurrence_id"
 "visit_detail_id"
 "procedure_source_value"
 "procedure_source_concept_id"
 "modifier_source_value"
"""
q = From(concept_ancestor)|>
Limit(1) |>
q -> render(q, dialect=OMOPCDMCohortCreator.dialect)
df=DBInterface.execute(conn, q) |> DataFrame
names(df)
"ancestor_concept_id"
"descendant_concept_id"
"min_levels_of_separation"
"max_levels_of_separation"



stmt = DBInterface.prepare(conn,q)

res = DBInterface.execute(conn, q)
DataFrame(res)

sql = FunSQL.From(procedure_occurrence) |>
Limit(1) |>
q -> render(q, dialect=OMOPCDMCohortCreator.dialect)

String(sql)






SELECT C.concept_id as ancestor_concept_id, C.concept_name as ancestor_concept_name, C.concept_code as ancestor_concept_code, C.concept_class_id as ancestor_concept_class_id, C.vocabulary_id, VA.vocabulary_name, A.min_levels_of_separation, A.max_levels_of_separation

FROM concept_ancestor A, concept C, vocabulary VA

WHERE A.ancestor_concept_id = C.concept_id

AND C.vocabulary_id = VA.vocabulary_id

AND A.ancestor_concept_id<>A.descendant_concept_id

AND A.descendant_concept_id = 192671

AND sysdate BETWEEN valid_start_date

AND valid_end_date

ORDER BY 5,7;