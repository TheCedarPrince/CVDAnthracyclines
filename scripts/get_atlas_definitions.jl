using HTTP
using JSON3
using OMOPCDMCohortCreator

include(scriptsdir("make_synthea_connection.jl"))

drugs = [
    (name = "Daunorubicin", id = 1311799),
    (name = "Doxorubicin", id = 1338512),
    (name = "Epirubicin", id = 1344354),
    (name = "Idarubicin", id = 19078097),
    (name = "Mitoxantrone", id = 1309188),
    (name = "Valrubicin", id = 19012543),
    (name = "Ibrutinib", id = 44507848),
    (name = "Acalabrutinib", id = 792764),
    (name = "Zanubrutinib", id = 37497691),
]

diseases = [
    (name = "Myocardial Infarction", id = 4329847),
    (name = "Heart Failure", id = 316139),
]

drug_descendants = []
for drug in drugs
    path = "https://atlas-demo.ohdsi.org/WebAPI/vocabulary/ATLASPROD/concept/$(drug.id)/descendants/"
    descendants = HTTP.get(path) |> x -> String(x.body) |> JSON3.read
    push!(drug_descendants, descendants)
end

disease_descendants = []
for disease in diseases
    path = "https://atlas-demo.ohdsi.org/WebAPI/vocabulary/ATLASPROD/concept/$(disease.id)/descendants/"
    descendants = HTTP.get(path) |> x -> String(x.body) |> JSON3.read
    push!(disease_descendants, descendants)
end

drug_users = Dict()
for (idx, drug) in enumerate(drugs)
    ids = DrugExposureFilterPersonIDs([drug[:CONCEPT_ID] for drug in drug_descendants[idx]], conn).person_id
    push!(drug_users, drug.name => (ids = ids, count = length(ids)))
end

diseased_patients = Dict()
for (idx, disease) in enumerate(diseases)
    ids = ConditionFilterPersonIDs([d[:CONCEPT_ID] for d in disease_descendants[idx]], conn).person_id
    push!(diseased_patients, disease.name => (ids = ids, count = length(ids)))
end

condition_mix = Dict()
for pg in keys(diseased_patients)
    push!(condition_mix, pg => Dict())
    for du in keys(drug_users)
        mix = intersect(diseased_patients[pg].ids, drug_users[du].ids)
        push!(condition_mix[pg], du => (ids = mix, count = length(mix)))
    end
end
        
open(datadir("exp_raw", "drug_user_counts.csv"), "w") do io
    write(io, "DRUG,COUNT\n")
    for du in keys(drug_users)
        write(io, "$du,$(drug_users[du].count)\n")
    end
end

open(datadir("exp_raw", "diseased_patient_counts.csv"), "w") do io
    write(io, "DISEASE,COUNT\n")
    for pg in keys(diseased_patients)
        write(io, "$pg,$(diseased_patients[pg].count)\n")
    end
end

open(datadir("exp_raw", "mix_counts.csv"), "w") do io
    write(io, "DISEASE,DRUG,COUNT\n")
    for disease in keys(condition_mix)
        for drug in keys(condition_mix[disease])
            write(io, "$disease,$drug,$(condition_mix[disease][drug].count)\n")
        end
    end
end
