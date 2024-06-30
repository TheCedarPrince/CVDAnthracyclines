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
includet("sql_functions/concept_utils.jl")
includet("sql_functions/conditions.jl")
includet("sql_functions/demographic_etc.jl")
includet("sql_functions/drugs.jl")
includet("sql_functions/labs.jl")
includet("sql_functions/observations_visits.jl")
includet("sql_functions/procedures.jl")

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


get_observation(conn, mio_infarct_patient_ids[1])


"""
we want to select single patient in the selected cohort and single visit of this patient
and display in human readle form all information about this patient including drugs,procedure, events and so on 
"""

drugs=get_drugs_of_person(conn, mio_infarct_patient_ids[9])
measuremants=get_measurement_per_person(conn, mio_infarct_patient_ids[1])[!,"concept_name"]
unique(measuremants)

conditions=get_condition_occurrence_of_person(conn, mio_infarct_patient_ids[4])[!,"concept_name"]
unique(conditions)



get_distinct_onco_conditions(conn)[!,"concept_name"]

get_distinct_device_exposure(conn)[!,"concept_name"]
get_distinct_procedure_occurrence(conn)[!,"concept_name"]

#general plan
"""
PLAN
1. get all patients with miocardial infarction based on manually selected conditions from atlas
2. select only those that have some oncologic conditions; based on conditions table
3. for each patient get all procedures, drugs, conditions, measurements
4. we try to map all of them to snomed and get distinct only 
5. we manually choose variables of intrest and group them
6. we are intrested for now with only patients that has miocardial infarction after oncologic diagnosis
7. from cohort in 6. we want to get subcohort of patients that has miocardial infarction also before oncologic diagnosis
"""

#drugs
"""
we want to map all drug concepts to atc as it supports grouping of drugs
look here https://forums.ohdsi.org/t/mapping-rxnorm-to-atc-with-mapping-in-concept-relationship/18104
so we want to get atc class in order to group all drugs of the same group like for example glicocorticosteroids
getting mapping of current drugs to atc is possible through relation and ancestor table 
1) we get all drugs from "omop.drug_era" table we return columns "person_id", "drug_concept_id", and the name of "drug_concept_id" that we get by 
    querying "omop.concept" table where "concept_id" is equal to "drug_concept_id"
2) we check what vocabulary is "drug_concept_id" in "omop.drug_era"  by checking "omop.vocabulary" and return in select 
    "vocabulary_name" from "omop.vocabulary" table where "vocabulary_id" is equal to "drug_concept_id" in "omop.drug_era" table
3) we analyze in "omop.concept_relationship" table rows where "concept_id_1" in  "omop.concept_relationship" is equal 
    to "drug_concept_id" in "omop.drug_era" we are intrested in  all relationships with string "- ATC" in in value of column "relationship_id" given "drug_concept_id"
    then we will get "concept_id_2" from "omop.concept_relationship" as one translated to atc;
4) we get ancestors of "concept_id_2" (by checking those that are equal to "ancestor_concept_id") in "omop.concept_ancestor" table and we return "ancestor_concept_id" and "ancestor_concept_name" 
    from "omop.concept" table where "concept_id" is equal to "ancestor_concept_id" in "omop.concept_ancestor" table and we return also "max_levels_of_separation" (we rename in return "max_levels_of_separation" into "max_levels_of_separation_ancestor") from "omop.concept_ancestor" table
5)we get ancestors of "concept_id_2" (by checking those that are equal to "descendant_concept_id") in "omop.concept_ancestor" table and we return "descendant_concept_id" and "descendant_concept_name" 
    from "omop.concept" table where "concept_id" is equal to "descendant_concept_id" in "omop.concept_ancestor" table and we return also "max_levels_of_separation" (we rename in return "max_levels_of_separation" into "max_levels_of_separation_dscendant") from "omop.concept_ancestor" table

"""
drugs_ids=get_drugs_of_person(conn, mio_infarct_patient_ids[3])[!,"drug_concept_id"]


df_for_atc=get_atc_level_of_drug(conn,drugs_ids[1])
ppath="/home/jakubmitura/projects/CVDAnthracyclines/data/debug_atc.csv"
CSV.write(ppath, df_for_atc)


drugs_ids=get_drugs_of_person(conn, mio_infarct_patient_ids[3])[!,"drug_concept_id"]

dff=get_drug_rx_to_atc_rel(conn,drugs_ids[3])
dff
idss=dff[!,"concept_id_2_id"]
get_ancestors(conn, idss[1])
get_standard_concept(conn, idss[1])
get_ancestors(conn, 21604180)

unique(get_drug_rx_to_atc_rel(conn, idss[1])[!,"relationship_id"])

df_anc=get_all_drug_ancestors(conn)
df_anc
unique(df_anc[!,"max_levels_of_separation"])
sep_level=2
df_anc[(df_anc.max_levels_of_separation .== sep_level) .& (df_anc.min_levels_of_separation .== sep_level), :]


length(dff[!,"relationship_name"])


ppath="/home/jakubmitura/projects/CVDAnthracyclines/data/debug.csv"
CSV.write(ppath, dff)


"concept_id_1_code","concept_id_2_code"

#procedures
"""
we want to map all to snomed get distinct and also look for ancestors to get more general procedures
"""


#measurements
"""
we want to map all to snomed get distinct and also look for ancestors to get more general measurements
"""

a



# get_ancestors(conn, 192671)

# sqq=""" SELECT C.concept_id as ancestor_concept_id, C.concept_name as ancestor_concept_name, VA.vocabulary_name, A.min_levels_of_separation, A.max_levels_of_separation
# FROM omop.concept_ancestor A, omop.concept C, omop.vocabulary VA
# WHERE A.ancestor_concept_id = C.concept_id
# AND C.vocabulary_id = VA.vocabulary_id
# AND A.descendant_concept_id = 192671
# AND A.ancestor_concept_id<>A.descendant_concept_id
# Limit 20 ;"""


# df=DBInterface.execute(conn, sqq) |> DataFrame


# """
# get concept id like for example procedure_type_concept_id then look for its ancestor in 
# concept_ancestor table aggregate to check what ancestor concepts are most common
# """
# sq=From(concept_ancestor) |>
# Join(:ancestor_concept_id => From(procedure_occurrence),
# Get.concept_ancestor.ancestor_concept_id .== Get.procedure_occurrence.procedure_type_concept_id, left=true) |>
# Group(Get.ancestor_concept_id) |>
# Select(Get.ancestor_concept_id,Agg.count()) |>  
# Order(Get.count |> Desc())|> 
# q -> render(q, dialect=OMOPCDMCohortCreator.dialect)
# df=DBInterface.execute(conn, sq) |> DataFrame





# From(concept_ancestor) |>
# Join(:ancestor_concept_id => From(procedure_occurrence),
# Get.concept_ancestor.ancestor_concept_id .==Get.procedure_occurrence.procedure_occurrence_id) |>
# q -> render(q, dialect=OMOPCDMCohortCreator.dialect)


# sq=From(concept_ancestor) |>
# Join(:ancestor_concept_id => From(procedure_occurrence),
# Get.concept_ancestor.ancestor_concept_id .==Get.procedure_occurrence.procedure_occurrence_id) |>
# Limit(2) |>
# q -> render(q, dialect=OMOPCDMCohortCreator.dialect)
# df=DBInterface.execute(conn, sq) |> DataFrame


# # |>
# # Select(Get.procedure_occurrence_id, Get.ancestor_concept_id)

# procedure_occurrence.procedure_occurrence_id concept_ancestor.ancestor_concept_id

# from(concept_ancestor, as=:A)
# join(concept, as=:C, on=:ancestor_concept_id => :concept_id)
# join(vocabulary, as=:VA, on=:C => :vocabulary_id => :VA => :vocabulary_id)



# select(
#     ancestor_concept_id = concept_id,
#     ancestor_concept_name = concept_name,
#     ancestor_concept_code = concept_code,
#     ancestor_concept_class_id = concept_class_id,
#     vocabulary_id,
#     vocabulary_name = vocabulary_name,
#     min_levels_of_separation,
#     max_levels_of_separation
# )
# from(concept_ancestor, as=:A)
# join(concept, as=:C, on=:ancestor_concept_id => :concept_id)
# join(vocabulary, as=:VA, on=:C => :vocabulary_id => :VA => :vocabulary_id)
# filter(
#     :A => :ancestor_concept_id != :A => :descendant_concept_id,
#     :A => :descendant_concept_id == 192671,
#     Fun.between(sysdate(), :valid_start_date, :valid_end_date)
# )
# order(:vocabulary_id, :min_levels_of_separation)

# """
# find ancestors of given concept based on https://github.com/OHDSI/OMOP-Queries/blob/master/md/General.md
# """
# function get_ancestors()

# end # get_ancestors  












# [[1,2],[1,3]]+[[1,2],[1,2]]










# sql = From(person) |> Select(Get.person_id) |> x -> render(x, dialect=OMOPCDMCohortCreator.dialect)
# DBInterface.execute(conn, sql) |> DataFrame



# q = From(procedure_occurrence)|>
# Select(Get.procedure_concept_id) |>
# q -> render(q, dialect=OMOPCDMCohortCreator.dialect)
# all_procedure_ids=DBInterface.execute(conn, q) |> DataFrame
# all_procedure_ids=unique(all_procedure_ids.procedure_concept_id)


# sample_id=all_procedure_ids[1]

# path = "https://atlas-demo.ohdsi.org/WebAPI/vocabulary/ATLASPROD/concept/$(sample_id)/ancestors/"
# ancestors = 
#     HTTP.get(path) |> 
#     x -> String(x.body) |> 
#     JSON3.read

  


# now i want to get back to ids of patiants with miocardial Infarction
# look through their procedures drugs events diseases in each case look 1 level up
# in hierarchy and print 20 most common procedures drugs events diseases





# q = From(procedure_occurrence)|>
# Limit(1) |>
# q -> render(q, dialect=OMOPCDMCohortCreator.dialect)
# df=DBInterface.execute(conn, q) |> DataFrame
# """
#  "procedure_occurrence_id"
#  "person_id"
#  "procedure_concept_id"
#  "procedure_date"
#  "procedure_datetime"
#  "procedure_end_date"
#  "procedure_end_datetime"
#  "procedure_type_concept_id"
#  "modifier_concept_id"
#  "quantity"
#  "provider_id"
#  "visit_occurrence_id"
#  "visit_detail_id"
#  "procedure_source_value"
#  "procedure_source_concept_id"
#  "modifier_source_value"
# """
# q = From(concept_ancestor)|>
# Limit(1) |>
# q -> render(q, dialect=OMOPCDMCohortCreator.dialect)
# df=DBInterface.execute(conn, q) |> DataFrame
# names(df)
# "ancestor_concept_id"
# "descendant_concept_id"
# "min_levels_of_separation"
# "max_levels_of_separation"



# stmt = DBInterface.prepare(conn,q)

# res = DBInterface.execute(conn, q)
# DataFrame(res)

# sql = FunSQL.From(procedure_occurrence) |>
# Limit(1) |>
# q -> render(q, dialect=OMOPCDMCohortCreator.dialect)

# String(sql)






# SELECT C.concept_id as ancestor_concept_id, C.concept_name as ancestor_concept_name, C.concept_code as ancestor_concept_code, C.concept_class_id as ancestor_concept_class_id, C.vocabulary_id, VA.vocabulary_name, A.min_levels_of_separation, A.max_levels_of_separation

# FROM concept_ancestor A, concept C, vocabulary VA

# WHERE A.ancestor_concept_id = C.concept_id

# AND C.vocabulary_id = VA.vocabulary_id

# AND A.ancestor_concept_id<>A.descendant_concept_id

# AND A.descendant_concept_id = 192671

# AND sysdate BETWEEN valid_start_date

# AND valid_end_date

# ORDER BY 5,7;