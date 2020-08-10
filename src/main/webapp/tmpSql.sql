SELECT `NAME`, code 
from component
where `DTYPE`="QueryComponent"
and `ITEM_ID` is null
order by id desc;
