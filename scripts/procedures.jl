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

## get info about device exposure (maybe radiotheraphy)
"""
table device_exposure
we look for row with value of a query in "person_id" column
    then print result from column_names
    ["device_exposure_start_date","device_exposure_end_date","device_concept_id","visit_occurrence_id"]
"""
function get_device_exposure(conn, person_id)
    sqq=""" SELECT device_exposure_start_date, device_exposure_end_date, device_concept_id, visit_occurrence_id
    FROM omop.device_exposure
    WHERE person_id = $(person_id);
    """
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_device_exposure


#procedure
"""
table procedure_occurrence
we can get procedures of the patient by looking for a row with patient_id in column "person_id"
    then when it was in column "procedure_date" what kind of procedure based on column "procedure_type_concept_id" 
    value and ranges so is it in range also based on column_names
    ["procedure_source_value","procedure_source_concept_id"]
"""
function get_procedure_occurrence(conn, person_id)
    sqq=""" SELECT procedure_date, procedure_type_concept_id, procedure_source_value, procedure_source_concept_id
    FROM omop.procedure_occurrence
    WHERE person_id = $(person_id);
    """
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_procedure_occurrence