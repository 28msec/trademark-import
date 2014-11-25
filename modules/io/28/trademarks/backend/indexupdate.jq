jsoniq version "1.0";

module namespace indexupdate = "http://trademarks.28.io/backend/indexupdate";

import module namespace updates = "http://trademarks.28.io/updates";
import module namespace s = "http://zorba.io/modules/data-cleaning/phonetic-string-similarity";
import module namespace ft = "http://zorba.io/modules/full-text";

declare variable $indexupdate:searchindex := "searchindex";

declare %an:sequential function indexupdate:process-all()
as object()?
{
    variable $result := ();
    
    while (true)
    {
        variable $next := indexupdate:process-some(5000);
        if (empty($next))
        then exit returning $result;
        else $result := $next;
    }
};

declare %an:sequential function indexupdate:process-some($limit as integer)
as object()?
{
    variable $upd := updates:request-updates("searchindex", $limit, true());
    if (exists($upd))
    then
        {
          for $trademark in updates:trademarks($upd)
          return indexupdate:update-entry($trademark);
          
          updates:confirm-updates($upd);
          
          $upd
        }
    else ()
};

declare %an:sequential function indexupdate:update-entry($trademark)
{
   variable $sn := $trademark.serial-number;
   variable $name := upper-case($trademark.case-file-header.mark-identification);
   variable $old := find($indexupdate:searchindex, { "_id" : $sn });   
   variable $entry :=
   {       
       "_id"  : $sn,
       "name" : $name,
       "type" : xs:string($trademark.case-file-header.standard-characters-claimed-in),
       "mi" : [ ft:tokenize-string($name) ],
       "mp" : [ ft:tokenize-string($name) ! s:metaphone-key($$) ],
       "stem" : [ ft:tokenize-string($name) !ft:stem($$) ],
       "sd" : s:soundex-key($name),
       "sn" : $sn,
       "rn" : $trademark.registration-number,
       "td" : $trademark.transaction-date,
       "rd" : $trademark.case-file-header.registration-date, 
       "fd" : $trademark.case-file-header.filing-date, (: make date? :)
       "iclass" : [ $trademark.classifications[].international-code[] ! xs:string($$) ],
       "own" : [ $trademark.case-file-owners[].party-name ! xs:string($$) ],
       "ownc" : [ $trademark.case-file-owners[].party-name ! xs:string(upper-case($$)) ],
       "ownt" : [ distinct-values($trademark.case-file-owners[].party-name ! ft:tokenize-string($$) ! upper-case($$)) ],
       "cor" : $trademark.correspondent.address-1,
       "corc" : xs:string(upper-case($trademark.correspondent.address-1)),
       "cort" : ft:tokenize-string(upper-case($trademark.correspondent.address-1)),
       "dead" : if (exists($trademark.case-file-header.abandonment-date) or exists($trademark.case-file-header.cancellation-date)) then true() else false(),      
       "at" : xs:string($trademark.case-file-header.attorney-name),
       "dc" : [ $trademark.design-searches[].code ]
   };
   
    let $existing := find($indexupdate:searchindex, { "_id" : $sn })
    return
        if (exists($existing))
        then edit($existing, $entry);
        else insert($indexupdate:searchindex, $entry);
};
      
 