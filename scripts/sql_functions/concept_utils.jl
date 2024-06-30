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
    sqq=""" SELECT C.concept_id as ancestor_concept_id,C.concept_name,A.min_levels_of_separation,A.max_levels_of_separation,VA.vocabulary_name
    FROM omop.concept_ancestor A, omop.concept C, omop.vocabulary VA
    WHERE A.ancestor_concept_id = C.concept_id
    AND C.vocabulary_id = VA.vocabulary_id
    AND A.descendant_concept_id = $(concept_id)
    AND A.ancestor_concept_id<>A.descendant_concept_id
    """
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_ancestors






## get concept name
"""
table concept
we look for row with value of a query in "concept_id" column
    then print result from column_names
    ["concept_name","concept_code","concept_class_id"]
"""
function get_concept_name(conn, concept_id)
    sqq=""" SELECT concept_name, concept_code, concept_class_id
    FROM omop.concept
    WHERE concept_id = $(concept_id)
    Limit 1 ;"""
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_concept_name


## get concept class name
"""
table concept_class
we look for row with value of a query in "concept_class_id" column
    then print result from column_name "concept_class_name"
"""
function get_concept_class_name(conn, concept_class_id)
    sqq=""" SELECT concept_class_name,concept_class_id
    FROM omop.concept_class
    WHERE concept_class_id = $(concept_class_id)
    Limit 1 ;"""
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_concept_class_name


#translation of code to standard vocabulary
"""
table source_to_standard_vocab_map
we look for row with value of a query in "source_concept_id" column
    then print result from column_names
    ["target_concept_id", "target_concept_name","target_vocabulary_id","target_domain_id" ]
additionaly we can get vocabulary name using 
vocabulary table and column_name "vocabulary_name" in row where "vocabulary_id" fit

"""
function get_standard_concept(conn, source_concept_id)
    sqq=""" SELECT target_concept_id, target_concept_name, target_vocabulary_id, target_domain_id
    FROM omop.source_to_standard_vocab_map
    WHERE source_concept_id = $(source_concept_id)
    Limit 1 ;"""
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_standard_concept


## get concept relation for mapping
""" 
table concept_relationship
we look for row with value of a query in "concept_id_1" column
    then print result from column_names
    ["concept_id_2","relationship_id"]
"""
function get_concept_relationship(conn, concept_id)
    sqq=""" SELECT concept_id_2, relationship_id
    FROM omop.concept_relationship
    WHERE concept_id_1 = $(concept_id)
    Limit 1 ;"""
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_concept_relationship


"""
we want to get all relationships of the concept we are interested in
so we look in table "omop.concept_relationship" for a row where "concept_id_1" is equal to
queried concept_id then we get "relationship_id" and "concept_id_2"
we return name of concept_id_1 and concept_id_2 that can be fould in table "omop.concept" like in example  ```SELECT concept_class_name,concept_class_id
    FROM omop.concept_class
    WHERE concept_class_id = concept_class_id)    Limit 1 ;```
and relationship name that can be found in table "omop.relationship" like in example ```SELECT relationship_name
    FROM omop.relationship
    WHERE relationship_id = relationship_id)   
     Limit 1 ;``    

"""
function get_related_concept_with_names(conn,concept_id_query)
    sqq=""" SELECT DISTINCT
    c1.concept_name AS concept_id_1_name, 
    c2.concept_name AS concept_id_2_name, 
    r.relationship_name,
    cr.relationship_id
    FROM 
        omop.concept_relationship cr
    JOIN 
        omop.concept c1 ON cr.concept_id_1 = c1.concept_id
    JOIN 
        omop.concept c2 ON cr.concept_id_2 = c2.concept_id
    JOIN 
        omop.relationship r ON cr.relationship_id = r.relationship_id
    WHERE 
        cr.concept_id_1 = $(concept_id_query) OR cr.concept_id_2 = $(concept_id_query);
        """
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_related_concept_with_names
    