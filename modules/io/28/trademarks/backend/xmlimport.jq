jsoniq version "1.0";

module namespace xmlimport = "http://trademarks.28.io/backend/xmlimport";

import module namespace http = "http://zorba.io/modules/http-client";
import module namespace archive = "http://zorba.io/modules/archive";
import module namespace html = "http://www.zorba-xquery.com/modules/converters/html";

declare variable $xmlimport:archives as string := "archives";
declare variable $xmlimport:trademarks as string := "trademarks";
declare variable $xmlimport:changes as string := "changes";
declare variable $xmlimport:errors as string := "errors";
declare variable $xmlimport:import-status as string := "import-status";

declare variable $xmlimport:created as integer := 0;
declare variable $xmlimport:updated as integer := 0;
declare variable $xmlimport:skipped as integer := 0;
declare variable $xmlimport:error as integer := 0;

declare variable $xmlimport:url as string := "";

declare variable $xmlimport:update-id as integer := 0;

declare %an:sequential function xmlimport:import-newest-missing() 
as object()?
{
    variable $missing := xmlimport:missing-archives()[1];
    if (exists($missing))
    then xmlimport:import-archive($missing)
    else ()
};

declare %an:sequential function xmlimport:import-historical-missing() 
as object()?
{
    variable $missing := xmlimport:missing-archives()[1];
    if (exists($missing))
    then xmlimport:import-archive($missing)
    else ()
};

declare %an:sequential function xmlimport:import-archive($url as xs:string) 
as object()
{
    $xmlimport:created := 0;
    $xmlimport:updated := 0;
    $xmlimport:skipped := 0;
    $xmlimport:error := 0;
    $xmlimport:update-id := xmlimport:get-update-id();
    $xmlimport:url := $url;
    variable $first-id := $xmlimport:update-id + 1;
    
    let $status := {
      status  : "running",
      started : current-dateTime(),
      href    : $url
    }
    return db:insert($xmlimport:archives, $status);
    flush();
    try {
    for $case at $idx in fn:parse-xml(archive:extract-text(http:get-binary($url).body.content)[1])//case-file
    return {
      xmlimport:import-case($case);
      store:flush-if($idx mod 1000 eq 0);
    } 
    
    flush();
    
    variable $status := find($xmlimport:archives, { href : $url });
    variable $status-new := {
      href    : $url,
      status  : "done",
      started : $status.started,
      finished : current-dateTime(),
      created : $xmlimport:created,
      updated : $xmlimport:updated,
      errors  : $xmlimport:error,
      skipped : $xmlimport:skipped,
      firstUpdate : $first-id,
      lastUpdate : $xmlimport:update-id
    };
    
    edit($status, $status-new);
    
    replace value of json collection($xmlimport:import-status).updateId with $xmlimport:update-id;
    flush();
    
    exit returning $status-new;
    } catch * {
        db:insert($xmlimport:errors, { archive : $url, code : $err:code, description : $err:description, line : $err:line-number, column : $err:column-number, module : $err:module });
        variable $status := find($xmlimport:archives, { href : $url });
        variable $status-new := {
              href    : $url,
              status  : "failed",
              started : $status.started,
              finished : current-dateTime(),
              created : $xmlimport:created,
              updated : $xmlimport:updated,
              errors  : $xmlimport:error,
              skipped : $xmlimport:skipped,
              firstUpdate : $first-id,
              lastUpdate : $xmlimport:update-id
        };
    
        edit($status, $status-new);
        
        replace value of json collection($xmlimport:import-status).updateId with $xmlimport:update-id;
        flush();
    
        exit returning $status-new;
    }
   
};

declare function xmlimport:get-update-id() 
as integer
{
    collection($xmlimport:import-status).updateId
};


declare function xmlimport:trademark($case as element(case-file)) as object()
{
    variable $serial-number := string($case/serial-number);
    variable $update-id := $xmlimport:update-id;
    
    {| { "_id" : $serial-number, uid : $update-id } , xmlimport:elem-to-object($case) |}
};

declare %an:sequential function xmlimport:import-case($case as element(case-file))
{
    try {
    variable $serial-number := string($case/serial-number);
    variable $transaction-date := string($case/transaction-date);
    
    variable $existing := find($xmlimport:trademarks, { "_id" : $serial-number });
    if (empty($existing))
    then 
    {
        $xmlimport:created := $xmlimport:created + 1;
        $xmlimport:update-id := $xmlimport:update-id + 1;
        db:insert($xmlimport:trademarks, xmlimport:trademark($case));
        db:insert($xmlimport:changes, { _id : $xmlimport:update-id , sn : $serial-number, ac:"created" });
    } 
    else if (xmlimport:timestamp($existing.transaction-date) lt xmlimport:timestamp($transaction-date))
    then 
    {
        $xmlimport:updated := $xmlimport:updated + 1;
        $xmlimport:update-id := $xmlimport:update-id + 1;
        
        let $old-markname := string($existing.case-file-header.mark-identification)
        let $new-markname := string($case/case-file-header/mark-identification) 
        return
            if ($old-markname eq $new-markname) 
            then ();
            else db:insert($xmlimport:changes, { _id : $xmlimport:update-id, sn : $serial-number ,ac:"renamed", old : $old-markname, new : $new-markname });
        
        edit($existing, xmlimport:trademark($case));
    }
     else 
    {
        $xmlimport:skipped := $xmlimport:skipped + 1;
    }
    } catch * { 
        $xmlimport:error := $xmlimport:error + 1;   
        db:insert($xmlimport:errors, { archive : $xmlimport:url, code : $err:code, description : $err:description, line : $err:line-number, column : $err:column-number, module : $err:module });
        flush();
    }
};

declare function xmlimport:timestamp($date as xs:string) as xs:string
{
     if (fn:matches($date,"^[0-9]+$"))
     then $date 
     else "0000"
};

declare %an:nondeterministic function xmlimport:available-archives()
as string*
{
    variable $response := http:get-text("http://www.google.com/googlebooks/uspto-trademarks-recent-applications.html");
    if ($response.status eq 200)
    then
        let $content := html:parse($response.body.content)
        for $href in data($content//a[starts-with(@href ,"http://storage.googleapis.com/trademarks/applications")]/@href)
        order by $href descending
        return $href
    else fn:error(xs:QName("xmlimport:httperror"), "Could not read trademark file list.")
};

declare %an:nondeterministic function xmlimport:historical-archives()
as string*
{
    variable $response := http:get-text("http://www.google.com/googlebooks/uspto-trademarks-recent-applications.html");
    if ($response.status eq 200)
    then
        let $content := html:parse($response.body.content)
        for $href in data($content//a[starts-with(@href ,"http://storage.googleapis.com/trademarks/retro/")]/@href)
        order by $href descending
        return $href
    else fn:error(xs:QName("xmlimport:httperror"), "Could not read trademark file list.")
};

declare function xmlimport:imported-archives()
as string*
{
    for $entry in collection($xmlimport:archives)
    where $entry.status eq "done"
    return $entry.href
};

declare function xmlimport:touched-archives()
as string*
{
    for $entry in collection($xmlimport:archives)
    where not($entry.status eq "retry")
    return $entry.href
};

declare %an:nondeterministic function xmlimport:missing-archives()
as string*
{
    let $imported := xmlimport:touched-archives()
    for $entry in (xmlimport:available-archives(), xmlimport:historical-archives())
    where not($entry = $imported)
    return $entry
};


declare %an:sequential function xmlimport:retry-failed-archives()
{
    for $archive in collection($xmlimport:archives)
    where $archive.status eq "failed" or $archive.errors gt 0
    return replace value of json $archive.status with "retry";
};

declare function xmlimport:elem-to-object($xml as element())
as object()
{
    {|
      for $tag in $xml/element()
      let $ln := local-name($tag)
      group by $ln
      return 
         
          if ($tag[1]/element())
          then 
              if (count($tag) gt 1) then fn:error(xs:QName("xmlimport:o"), $ln)
              else
              if ($ln = ("case-file-statements","case-file-event-statements","case-file-owners","classifications","madrid-history-events","madrid-international-filing-requests","design-searches", "prior-registration-applications", "foreign-applications"))
              then { $ln : xmlimport:elem-to-array($tag) }
              else try { { $ln : xmlimport:elem-to-object($tag) } } catch xmlimport:o { fn:error(xs:QName("xmlimport:ob"), $ln ) }
          else 
              if ($ln = "transaction-date")
              then { transaction-date : xmlimport:timestamp($tag) }
              else
              if ($ln = ("us-code", "name-change-explanation","international-code"))
              then { $ln : [ for $t in $tag return string($t) ] }
              else 
                  if (count($tag) gt 1) then fn:error(xs:QName("xmlimport:d"), $ln)
                  else { $ln : string($tag) }
    |}
};

declare function xmlimport:elem-to-array($xml as element())
as array()
{
    [
      for $elem in $xml/element()
      return 
          if ($elem/element()) then xmlimport:elem-to-object($elem)
          else { value : string($elem) }
    ]
};

