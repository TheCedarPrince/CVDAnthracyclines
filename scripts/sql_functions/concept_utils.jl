using DrWatson
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
