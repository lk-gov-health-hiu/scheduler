SELECT `ID`,`NAME`, code 
from Item
where `CODE` = "BMI"
order by id desc;
select count(*)
from component
where `DTYPE`="ClientEncounterComponentItem"
and `ITEM_ID`=385931;
select id, `name`,`REALNUMBERVALUE`,`REALNUMBERVALUE2`, `QUERYDATATYPE`,`RETIRED`,`PARENTCOMPONENT_ID`
from component
where `DTYPE`="QueryComponent"
and `ITEM_ID`=385931
order by `name`;
