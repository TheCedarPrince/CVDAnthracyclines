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
    sqq=""" SELECT de.drug_concept_id,c.concept_name,de.drug_era_start_date, de.drug_era_end_date
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









"""
want to get all relationships of the drugs with atc in name
"""
function get_all_atc_relationships_of_drugs(conn)
    sqq="""SELECT DISTINCT
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
    r.relationship_name ILIKE '%atc%'
    AND (cr.concept_id_1 IN (SELECT DISTINCT drug_concept_id FROM omop.drug_era)
         OR cr.concept_id_2 IN (SELECT DISTINCT drug_concept_id FROM omop.drug_era));
          """
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_related_concept_with_names
    
"""
get rx_norm to atc mapping concepts
"""
function get_drug_rx_to_atc_rel(conn,concept_id_query)
    sqq=""" SELECT DISTINCT
    c1.concept_name AS concept_id_1_name, 
    c2.concept_name AS concept_id_2_name, 
    c1.concept_id AS concept_id_1_id, 
    c2.concept_id AS concept_id_2_id, 
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
(cr.concept_id_1 = $(concept_id_query) OR cr.concept_id_2 = $(concept_id_query))
    AND r.relationship_name ILIKE '%rxnorm%'
    AND r.relationship_name ILIKE '%atc%'    ;
        """
    df=DBInterface.execute(conn, sqq) |> DataFrame
    return df
end # get_related_concept_with_names



function get_all_drug_ancestors(conn)
    sqq="""
        SELECT C.concept_id as ancestor_concept_id, C.concept_name as ancestor_name,C2.concept_name as descendant_name, A.min_levels_of_separation, A.max_levels_of_separation, VA.vocabulary_name
        FROM omop.concept_ancestor A, omop.concept C,omop.concept C2, omop.vocabulary VA
        WHERE A.ancestor_concept_id = C.concept_id
        AND A.descendant_concept_id = C2.concept_id
        AND C.vocabulary_id = VA.vocabulary_id
        AND A.descendant_concept_id IN (SELECT DISTINCT drug_concept_id FROM omop.drug_era)
        AND A.ancestor_concept_id <> A.descendant_concept_id;
        """
df=DBInterface.execute(conn, sqq) |> DataFrame
return df
end # get_all_drug_ancestors


"""
we want to map all drug concepts to atc as it supports grouping of drugs
look here https://forums.ohdsi.org/t/mapping-rxnorm-to-atc-with-mapping-in-concept-relationship/18104
so we want to get atc class in order to group all drugs of the same group like for example glicocorticosteroids
getting mapping of current drugs to atc is possible through relation and ancestor table 
1) we get all drugs from "omop.drug_era" table we return columns "person_id", "drug_concept_id", and the name of "drug_concept_id" that we get by 
    querying "omop.concept" table where "concept_id" is equal to "drug_concept_id"
2) we check what vocabulary is "drug_concept_id" in "omop.drug_era"  by checking "omop.vocabulary" and return in select 
    "vocabulary_name" from "omop.vocabulary" table where "vocabulary_id" is equal to "drug_concept_id" in "omop.drug_era" table
3) we analyze in "omop.concept_relationship" table rows where "concept_id_1" in  "omop.concept_relationship" is equal 
    to "drug_concept_id" in "omop.drug_era" we are intrested in  all relationships with string "- ATC" in in value of column "relationship_id" given "drug_concept_id"
    then we will get "concept_id_2" from "omop.concept_relationship" as one translated to atc;
4) we get ancestors of "concept_id_2" (by checking those that are equal to "ancestor_concept_id") in "omop.concept_ancestor" table and we return "ancestor_concept_id" and "ancestor_concept_name" 
    from "omop.concept" table where "concept_id" is equal to "ancestor_concept_id" in "omop.concept_ancestor" table and we return also "max_levels_of_separation" (we rename in return "max_levels_of_separation" into "max_levels_of_separation_ancestor") from "omop.concept_ancestor" table
5)we get ancestors of "concept_id_2" (by checking those that are equal to "descendant_concept_id") in "omop.concept_ancestor" table and we return "descendant_concept_id" and "descendant_concept_name" 
    from "omop.concept" table where "concept_id" is equal to "descendant_concept_id" in "omop.concept_ancestor" table and we return also "max_levels_of_separation" (we rename in return "max_levels_of_separation" into "max_levels_of_separation_dscendant") from "omop.concept_ancestor" table

"""
function get_atc_level_of_drug(conn,drug_concept_id)
     sqq="""WITH DrugInfo AS (
    SELECT 
        de.person_id, 
        de.drug_concept_id, 
        c.concept_name AS drug_name,
        v.vocabulary_name
    FROM 
        omop.drug_era de
    JOIN 
        omop.concept c ON de.drug_concept_id = c.concept_id
    JOIN 
        omop.vocabulary v ON c.vocabulary_id = v.vocabulary_id
    WHERE drug_concept_id=$(drug_concept_id)   
),
ATCRelations AS (
    SELECT 
        cr.concept_id_1, 
        cr.concept_id_2
    FROM 
        omop.concept_relationship cr
    JOIN 
        omop.relationship r ON cr.relationship_id = r.relationship_id
    WHERE 
        r.relationship_id ILIKE '%- ATC%'
    AND 
        cr.concept_id_1 IN (SELECT DISTINCT drug_concept_id FROM omop.drug_era)
),
Ancestors AS (
    SELECT 
        ca.ancestor_concept_id, 
        c.concept_name AS ancestor_concept_name, 
        ca.max_levels_of_separation AS max_levels_of_separation_ancestor
    FROM 
        omop.concept_ancestor ca
    JOIN 
        omop.concept c ON ca.ancestor_concept_id = c.concept_id
    WHERE 
        ca.ancestor_concept_id IN (SELECT concept_id_2 FROM ATCRelations)
),
Descendants AS (
    SELECT 
        ca.descendant_concept_id, 
        c.concept_name AS descendant_concept_name, 
        ca.max_levels_of_separation AS max_levels_of_separation_descendant
    FROM 
        omop.concept_ancestor ca
    JOIN 
        omop.concept c ON ca.descendant_concept_id = c.concept_id
    WHERE 
        ca.descendant_concept_id IN (SELECT concept_id_2 FROM ATCRelations)
)
SELECT 
    di.person_id, 
    di.drug_concept_id, 
    di.drug_name, 
    di.vocabulary_name, 
    a.ancestor_concept_id, 
    a.ancestor_concept_name, 
    a.max_levels_of_separation_ancestor, 
    d.descendant_concept_id, 
    d.descendant_concept_name, 
    d.max_levels_of_separation_descendant
FROM 
    DrugInfo di
JOIN 
    ATCRelations ar ON di.drug_concept_id = ar.concept_id_1
LEFT JOIN 
    Ancestors a ON ar.concept_id_2 = a.ancestor_concept_id
LEFT JOIN 
    Descendants d ON ar.concept_id_2 = d.descendant_concept_id
     """
     df=DBInterface.execute(conn, sqq) |> DataFrame
return df
end # get_atc_level