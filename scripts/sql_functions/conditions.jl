using DrWatson
using JSON3,Revise
using HTTP
using OMOPCDMCohortCreator
using CSV
using DataFrames
using DBInterface
import DBInterface:
    connect,
    execute
using FunSQL
using FunSQL: From,Limit,render,Get,Select,Join,LeftJoin,Agg,Group,Order,Asc,Desc


## get into about conditions 
"""
table condition_occurrence
we look for patient id in column "person_id" and condition id in column "condition_concept_id"
then we want to print when it was given so we look for "condition_start_date" and "condition_end_date"
"""
function get_condition_occurrence_of_person(conn, person_id)
    sqq=""" SELECT c.concept_name,co.condition_start_date, co.condition_end_date
    FROM omop.condition_occurrence co
    JOIN omop.concept c ON co.condition_concept_id = c.concept_id
    WHERE person_id = $(person_id);
    """
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_condition_occurrence

# DISTINCT
function get_distinct_onco_conditions(conn)
    sqq=""" SELECT DISTINCT c.concept_name
FROM omop.condition_occurrence co
JOIN omop.concept c ON co.condition_concept_id = c.concept_id
WHERE c.concept_name ILIKE '%onco%'
   OR c.concept_name ILIKE '%cancer%'
   OR c.concept_name ILIKE '%metastasis%'
   OR c.concept_name ILIKE '%neoplasm%';    """
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_condition_occurrence
