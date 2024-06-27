using DrWatson
@quickactivate "CVDAnthracyclines"
using JSON3
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


"""
we created manually a DataFrame where in given column of name "col_name" 
we have put manually 1's if we accept the code and Id of the concept is "Id"
dataframe is based on csv downloaded from atlas query and modified manually
"""
function get_manually_accepted_from_df(csv_path,col_name)
    miocard_df = CSV.File(csv_path) |> DataFrame
    df=miocard_df[!,["Id","Name",col_name,"Vocabulary"]]
    df = df[.!ismissing.(df[!,col_name]), :]
    df = df[.!ismissing.(df.Id), :]
    df_was = df[df[!,col_name] .== 1, :]
    # unique(df_was[!,"was"])
    df_was[!,"Id"]

end   


miocard_path="/home/jakubmitura/projects/CVDAnthracyclines/data/ATLAS_miocardial_infarction.csv"
col_name="was"
get_manually_accepted_from_df(miocard_path,col_name)

"""
get a list of ancestor concepts of a given concept
adapted from https://github.com/OHDSI/OMOP-Queries/blob/master/md/General.md
"""
function get_ancestors(conn, concept_id)
    sqq=""" SELECT C.concept_id as ancestor_concept_id
    FROM omop.concept_ancestor A, omop.concept C, omop.vocabulary VA
    WHERE A.ancestor_concept_id = C.concept_id
    AND C.vocabulary_id = VA.vocabulary_id
    AND A.descendant_concept_id = $(concept_id)
    AND A.ancestor_concept_id<>A.descendant_concept_id
    Limit 20 ;"""
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df[!,:ancestor_concept_id]
end # get_ancestors

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