SELECT count(*) FROM consolidatedqueryresult 
where longvalue is null
order by id desc
LIMIT 100;
SELECT count(*) FROM individualqueryresult 
where included is null
order by id desc
LIMIT 100;
SELECT id, `CREATEDAT`, `PROCESSSTARTED`, `PROCESSCOMPLETED`, `SUBMITTEDFORCONSOLIDATION`, `READYAFTERCONSOLIDATION`
 FROM storedqueryresult 
order by id desc
LIMIT 10;
