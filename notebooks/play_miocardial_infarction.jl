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
using FunSQL: From,Limit,render,Get,Select

conn = connect(
    Connection, 
    "host=localhost port=5432 dbname=synthea user=thecedarprince password=JJJOjjjo1*"
)    
GenerateDatabaseDetails(:postgresql, "omop")
tables = GenerateTables(conn, inplace = false, exported = true)
person = tables[:person]
procedure_occurrence = tables[:procedure_occurrence]


sql = From(person) |> Select(Get.person_id) |> x -> render(x, dialect=OMOPCDMCohortCreator.dialect)
DBInterface.execute(conn, sql) |> DataFrame



q = From(procedure_occurrence)|>
Select(Get.procedure_concept_id) |>
q -> render(q, dialect=OMOPCDMCohortCreator.dialect)
all_procedure_ids=DBInterface.execute(conn, q) |> DataFrame
all_procedure_ids=unique(all_procedure_ids.procedure_concept_id)


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


stmt = DBInterface.prepare(conn,q)

res = DBInterface.execute(conn, q)
DataFrame(res)

sql = FunSQL.From(procedure_occurrence) |>
Limit(1) |>
q -> render(q, dialect=OMOPCDMCohortCreator.dialect)

String(sql)