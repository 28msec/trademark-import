jsoniq version "1.0";

module namespace updates = "http://trademarks.28.io/updates";
import module namespace trademarks = "http://trademarks.28.io/trademarks";
import module namespace mongo = "http://www.28msec.com/modules/mongodb";

declare variable $updates:statuscollection := "update-status";
declare variable $updates:timeout := xs:dayTimeDuration("P1D");

declare function updates:max-update-id()
as integer
{
    mongo:find($trademarks:connection, "import-status").updateId
};

declare %an:sequential function updates:request-updates($service-name as string, $limit as integer, $allow-parallelization as boolean)
as object()?
{
    if ($allow-parallelization)
    then ();
    else 
        if (exists(
             for $entry in find($updates:statuscollection, { service : $service-name, status : "running" })
             where current-dateTime() - $entry.started lt $updates:timeout
             return $entry))
        then fn:error(xs:QName("updates:already-running"),"Service '" || $service-name || "' is already running.");
        else ();
    variable $old-fails := 
      for $entry in find($updates:statuscollection, { service : $service-name })
      where $entry.status eq "failed" 
      or ($entry.status eq "running" and (current-dateTime() - $entry.started gt $updates:timeout) )
      return $entry;
      
    if (exists($old-fails))
    then
    {
        variable $old := $old-fails[1];
        replace value of json $old.started with current-dateTime();
        replace value of json $old.status with "running";
        flush();
        $old
    }
    else 
    {
        variable $old-success := 
          (
          for $entry in find($updates:statuscollection, { service : $service-name })
          where $entry.status = ("running", "success")
          order by $entry.lastUpdateId descending
          return $entry
          )[1];
          
         variable $first-update-id :=
            if (exists($old-success))
            then $old-success.lastUpdateId
            else 1;
         variable $last-update-id :=
            if ($first-update-id eq updates:max-update-id())
            then null
            else
            if ($first-update-id + $limit gt updates:max-update-id())
            then updates:max-update-id()
            else $first-update-id + $limit;
        
         if ($last-update-id eq null)
         then ()
         else
         {
         variable $result := db:apply-insert($updates:statuscollection, 
              { 
                 service : $service-name,
                 firstUpdateId : $first-update-id,
                 lastUpdateId : $last-update-id,
                 status : "running",
                 started : current-dateTime()
              }
            );
            flush();
            $result
         }
    }
        
};

declare %an:sequential function updates:confirm-updates($update-obj)
{
    replace value of json $update-obj.status with "success";
    flush();
    updates:merge($update-obj.service);
};

declare %an:sequential function updates:mark-failed($update-obj)
{
    replace value of json $update-obj.status with "failed";
    flush();
};

declare %an:sequential function updates:reset-to-beginning($service-name as string)
{
    db:delete(find($updates:statuscollection, { service : $service-name }));
    flush();
};

declare %private %an:sequential function updates:merge($service-name as string)
{
    let $successes := 
      for $entry at $idx in find($updates:statuscollection, { service : $service-name, status : "success" })
      order by $entry.lastUpdateId ascending
      return $entry
    for $success at $idx in $successes
    where $idx gt 1
    return
        if ($successes[$idx].firstUpdateId eq $successes[$idx - 1].lastUpdateId)
        then {
           replace value of json $successes[$idx].firstUpdateId with $successes[$idx - 1].firstUpdateId;
           db:delete($successes[$idx -1 ]);
           flush();
        } else ();
};

declare function updates:trademarks($updates as object())
as object()*
{
    mongo:find($trademarks:connection, "trademarks", { uid : { "$gt": $updates.firstUpdateId - 1, "$lt": $updates.lastUpdateId } } )
};

declare function updates:added-trademarks($updates as object())
as object()*
{
    for $upd in mongo:find($trademarks:connection, "changes", { _id : { "$gt": $updates.firstUpdateId - 1, "$lt": $updates.lastUpdateId } , ac : "created" } )
    let $trademark := mongo:find($trademarks:connection,"trademarks", { _id : $upd.sn })
    return $trademark
};


declare function updates:renamed-trademarks($updates as object())
as object()*
{
    for $upd in mongo:find($trademarks:connection, "changes", { _id : { "$gt": $updates.firstUpdateId - 1, "$lt": $updates.lastUpdateId } , ac : "renamed" } )
    let $trademark := mongo:find($trademarks:connection,"trademarks", { _id : $upd.sn })
    return {| { old-mark-identification : $upd.old } , $trademark |}
};

