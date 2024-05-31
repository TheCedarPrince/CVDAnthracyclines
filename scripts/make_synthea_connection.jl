using DBInterface
using LibPQ 
using OMOPCDMCohortCreator

conn = DBInterface.connect(
    LibPQ.Connection, 
    "user=thecedarprince dbname=synthea"
)

GenerateDatabaseDetails(:postgresql, "omop")
GenerateTables(conn)
