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
import LibPQ:
    Connection
using FunSQL
using FunSQL: From,Limit,render,Get,Select,Join,LeftJoin,Agg,Group,Order,Asc,Desc



#total observation time
"""
table observation_period
get the total observation period of the patient by looking for a row with patient_id in column "person_id" 
then subtract value of the date from column "observation_period_end_date" nad column  and  "observation_period_start_date"
"""
function get_observation_period(conn, person_id)
    sqq=""" SELECT observation_period_end_date, observation_period_start_date
    FROM omop.observation_period
    WHERE person_id = $(person_id)
    Limit 1 ;"""
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_observation_period


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


## get info about visit of a patient
"""
table visit_detail
look for patient of id in column "person_id" and visit id in column "visit_occurrence_id"
then we want to print when it was in column "visit_start_date" and "visit_end_date"
"""
function get_visit_info(conn, person_id)
    sqq=""" SELECT visit_start_date, visit_end_date
    FROM omop.visit_occurrence
    WHERE person_id = $(person_id)
    Limit 1 ;"""
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_visit_info


## get more info about visit of a patient
"""
table visit_occurrence
look for patient of id in column "person_id" and visit id in column "visit_occurrence_id"
then we want to print when it was in column "visit_start_date" and "visit_end_date"
"""
function get_visit_info(conn, person_id)
    sqq=""" SELECT visit_start_date, visit_end_date
    FROM omop.visit_occurrence
    WHERE person_id = $(person_id)
    Limit 1 ;"""
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_visit_info