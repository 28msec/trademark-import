import module namespace xmlimport = "http://trademarks.28.io/backend/xmlimport";

for $x in 1 to 6
return xmlimport:import-newest-missing()