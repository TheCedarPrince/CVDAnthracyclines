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


## get dose of the drug
"""
table dose_era
we look for a dose for a given drug ogf given person in given time
so we look for a row with queried patient id from column "person_id"   
in dose era "dose_era_id"   we want to also print concept name on the basis of
"drug_concept_id" column in the end we want dose value from column "dose_value"
"""
function get_dose_era(conn, person_id)
    # sqq="""SELECT de.dose_era_id, c.concept_name, de.dose_value
    #     FROM omop.dose_era de
    #     JOIN omop.concept c ON de.drug_concept_id = c.concept_id
    #     WHERE de.person_id = $(person_id);
    # """
    sqq=""" SELECT dose_era_id, drug_concept_id, dose_value
    FROM omop.dose_era
    WHERE person_id = $(person_id);
    """
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_dose_era

## get info when drug was given in given dose
"""
table dose_era
we look for patient id in column "person_id" and drug id in column "drug_concept_id"
then we want to print when it was given so we look for "dose_era_start_date" and "dose_era_end_date"
"""
function get_dose_era_start_end(conn, person_id, drug_concept_id)
    sqq=""" SELECT dose_era_start_date, dose_era_end_date
    FROM omop.dose_era
    WHERE person_id = $(person_id)
    AND drug_concept_id = $(drug_concept_id)
    Limit 1 ;"""
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_dose_era_start_end

## get info when drug was given irrespective of dose
"""
table drug_era
we look for patient id in column "person_id" and drug id in column "drug_concept_id"
then we want to print when it was given so we look for "drug_era_start_date" and "drug_era_end_date"
"""
function get_drugs_of_person(conn, person_id)
    sqq=""" SELECT c.concept_name,de.drug_era_start_date, de.drug_era_end_date
    FROM omop.drug_era de
    JOIN omop.concept c ON de.drug_concept_id = c.concept_id
    WHERE person_id = $(person_id);
"""


    # sqq=""" SELECT drug_era_start_date, drug_era_end_date
    # FROM omop.drug_era
    # WHERE person_id = $(person_id)
    # AND drug_concept_id = $(drug_concept_id)
    # Limit 1 ;"""
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_drug_era_start_end
