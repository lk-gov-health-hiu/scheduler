SELECT `ITEM_ID`, count(*) FROM 
component
where `DTYPE`="ClientEncounterComponentItem"
and `REALNUMBERVALUE` is not null
group by `ITEM_ID`;
