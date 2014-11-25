jsoniq version "1.0";

module namespace trademarks = "http://trademarks.28.io/trademarks";
import module namespace mongo = "http://www.28msec.com/modules/mongodb";

declare variable $trademarks:connection := mongo:connect();

declare function trademarks:get-by-serial-number($serial-number as integer)
as object()?
{
    find($trademarks:connection, "trademarks", { _id : $serial-number })
};