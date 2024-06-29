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

## get info about death
"""
table death
we look for row with value of a query in "person_id" column
    then print result from column_names
    ["death_date","cause_concept_id"]
"""
function get_death_info(conn, person_id)
    sqq=""" SELECT death_date, cause_concept_id
    FROM omop.death
    WHERE person_id = $(person_id)
    Limit 1 ;"""
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_death_info

## get demographic info about patient
"""
table person
we look for patient id in column "person_id" and we want to print
    columns["gender_concept_id","year_of_birth" ]
"""
function get_demographic_info(conn, person_id)
    sqq="""SELECT gender_concept_id, year_of_birth
    FROM omop.person
    WHERE person_id = YOUR_PATIENT_ID"""
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_demographic_info