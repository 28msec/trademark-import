import module namespace xmlimport = "http://trademarks.28.io/backend/xmlimport";
import module namespace updates = "http://trademarks.28.io/updates";
import module namespace indexupdate = "http://trademarks.28.io/backend/indexupdate";

if (is-available-collection($xmlimport:archives))
then truncate($xmlimport:archives);
else create($xmlimport:archives);

if (is-available-collection($xmlimport:trademarks))
then truncate($xmlimport:trademarks);
else create($xmlimport:trademarks);

if (is-available-collection($xmlimport:changes))
then truncate($xmlimport:changes);
else create($xmlimport:changes);

if (is-available-collection($xmlimport:errors))
then truncate($xmlimport:errors);
else create($xmlimport:errors);

if (is-available-collection($xmlimport:import-status))
then truncate($xmlimport:import-status);
else create($xmlimport:import-status);

if (is-available-collection($updates:statuscollection))
then truncate($updates:statuscollection);
else create($updates:statuscollection);

if (is-available-collection($indexupdate:searchindex))
then truncate($indexupdate:searchindex);
else create($indexupdate:searchindex);

db:insert($xmlimport:import-status, { updateId : 0 });