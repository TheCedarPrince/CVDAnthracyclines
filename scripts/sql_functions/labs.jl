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

#measurements
"""
table measurement
we can get measurements of the patient by looking for a row with patient_id in column "person_id"
    then when it was in column "measurement_date" what kind of measurement based on column "measurement_type_concept_id" 
    value "value_as_number" and ranges so is it in range also based on column_names
    ["range_low","range_high"]
"""
function get_measurement_per_person(conn, person_id)
    sqq=""" SELECT me.measurement_date, c.concept_name, me.value_as_number, me.range_low, me.range_high
    FROM omop.measurement me
    JOIN omop.concept c ON me.measurement_concept_id = c.concept_id
    WHERE person_id = $(person_id);
    """
    #measurement_type_concept_id, value_as_concept_id
    # sqq=""" SELECT me.measurement_date, measurement_type_concept_id, value_as_number, range_low, range_high
    # FROM omop.measurement me
    # WHERE person_id = $(person_id);
    # """
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_measurement
