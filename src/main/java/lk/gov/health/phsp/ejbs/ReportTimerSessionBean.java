package lk.gov.health.phsp.ejbs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.ejb.EJB;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.ejb.Stateless;
import lk.gov.health.phsp.entity.ClientEncounterComponentItem;
import lk.gov.health.phsp.entity.ConsolidatedQueryResult;
import lk.gov.health.phsp.entity.IndividualQueryResult;
import lk.gov.health.phsp.entity.Institution;
import lk.gov.health.phsp.entity.Item;
import lk.gov.health.phsp.entity.QueryComponent;
import lk.gov.health.phsp.entity.Upload;
import lk.gov.health.phsp.enums.EncounterType;
import lk.gov.health.phsp.enums.QueryCriteriaMatchType;
import lk.gov.health.phsp.enums.QueryLevel;
import lk.gov.health.phsp.entity.StoredQueryResult;
import lk.gov.health.phsp.facade.ClientEncounterComponentItemFacade;
import lk.gov.health.phsp.facade.ConsolidatedQueryResultFacade;
import lk.gov.health.phsp.facade.EncounterFacade;
import lk.gov.health.phsp.facade.IndividualQueryResultFacade;
import lk.gov.health.phsp.facade.QueryComponentFacade;
import lk.gov.health.phsp.facade.StoredQueryResultFacade;
import lk.gov.health.phsp.facade.UploadFacade;
import lk.gov.health.phsp.pojcs.ReportTimePeriod;
import org.apache.commons.io.FileUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import lk.gov.health.phsp.pojcs.ClientEncounterComponentBasicDataToQuery;
import org.apache.commons.io.IOUtils;

/**
 *
 * @author buddhika
 */
@Singleton
public class ReportTimerSessionBean {

    boolean logActivity = true;
    @EJB
    private StoredQueryResultFacade storeQueryResultFacade;
    @EJB
    private UploadFacade uploadFacade;
    @EJB
    private EncounterFacade encounterFacade;
    @EJB
    private QueryComponentFacade queryComponentFacade;
    @EJB
    private ClientEncounterComponentItemFacade clientEncounterComponentItemFacade;
    @EJB
    private ConsolidatedQueryResultFacade consolidatedQueryResultFacade;
    @EJB
    private IndividualQueryResultFacade individualQueryResultFacade;

    private List<QueryComponent> queryComponents;

//    @Schedule(
//            hour = "*",
//            minute = "*/3",
//            second = "0",
//            persistent = false)
    public void runReports() {
        if (logActivity) {
            System.out.println("Going to run reports at " + currentTimeAsString() + ".");
        }
        submitToConsilidate();
        checkCompletenessAfterConsolidation();
        generateFileAfterConsolidation();
        createConsolidatedResultsFromIndividualResults();
        createIndividualResultsForConsolidatedResults();
        runIndividualQuerys();
    }

    private void runIndividualQuerys() {
        if (logActivity) {
            System.out.println("Going to run individual queries at " + currentTimeAsString() + ".");
        }
        int singleProcessCount = 1000;
        List<IndividualQueryResult> cqrs;
        String j;
        j = "select r "
                + " from IndividualQueryResult r "
                + " where r.included is null "
                + " order by r.id "
                + " ";
        cqrs = getIndividualQueryResultFacade().findByJpql(j, singleProcessCount);

        if (cqrs == null) {
            if (logActivity) {
                System.out.println("No individual queries left to process.");
            }
            return;
        }

        if (logActivity) {
            System.out.println("Number of individual queries processing this time is " + cqrs.size());
        }
        for (IndividualQueryResult r : cqrs) {
            calculateIndividualQueryResult(r);
            for (int i = 0; i < 1000; i++) {
                if (i % 1000 == 1) {
                    System.out.print(".");
                }
            }
        }

    }

    private void createIndividualResultsForConsolidatedResults() {
        if (logActivity) {
            System.out.println("Going to create individual queries at " + currentTimeAsString());
        }
        int singleProcessCount = 20;
        List<ConsolidatedQueryResult> cqrs;
        String j;
        j = "select r "
                + " from ConsolidatedQueryResult r "
                + " where r.longValue is null "
                + " order by r.id "
                + " ";
        cqrs = getConsolidatedQueryResultFacade().findByJpql(j, singleProcessCount);

        if (cqrs == null) {
            if (logActivity) {
                System.out.println("No consolidated queries to create individual queries.");
            }
            return;
        }
        List<Long> encIds = new ArrayList<>();
        Long lastInsId = 0l;
        Date lastFrom = new Date();
        Date lastTo = new Date();
        if (logActivity) {
            System.out.println("Number of Consolidated queries to create individual queries is " + cqrs.size());
        }
        for (ConsolidatedQueryResult r : cqrs) {

            if (!lastInsId.equals(r.getInstitution().getId()) || !lastFrom.equals(r.getResultFrom()) || !lastTo.equals(r.getResultTo())) {
                encIds = findEncounterIds(r.getResultFrom(), r.getResultTo(), r.getInstitution());
                lastInsId = r.getInstitution().getId();
                lastFrom = r.getResultFrom();
                lastTo = r.getResultTo();
            }

            Long lastIndividualQueryResultId = 0l;
            if (encIds == null) {
                if (logActivity) {
                    System.out.println("No Encounters for consolidated query for " + r.getInstitution().getName());
                }
                r.setLongValue(0l);
                getConsolidatedQueryResultFacade().edit(r);
                continue;
            }
            if (encIds.isEmpty()) {
                if (logActivity) {
                    System.out.println("No Encounters for consolidated query for " + r.getInstitution().getName());
                }
                r.setLongValue(0l);
                getConsolidatedQueryResultFacade().edit(r);
                continue;
            }
//            if(logActivity) System.out.println("Number of Encounters for consolidated query for " + r.getInstitution().getName() + " is " + encIds.size() );
            for (Long encId : encIds) {
                Long generatedId = createIndividualQueryResultsForConsolidateQueryResult(encId, r.getQueryComponentCode(), r.getQueryComponentId());
                if (generatedId > lastIndividualQueryResultId) {
                    lastIndividualQueryResultId = generatedId;
                }
            }
            r.setLastIndividualQueryResultId(lastIndividualQueryResultId);
            getConsolidatedQueryResultFacade().edit(r);
            for (int i = 0; i < 1000; i++) {
                if (i % 1000 == 1) {
                    System.out.print(".");
                }
            }
        }

    }

    private void createConsolidatedResultsFromIndividualResults() {
        if (logActivity) {
            System.out.println("Creating consolidated results from individual queries at " + currentTimeAsString());
        }
        int singleProcessCount = 20;
        List<ConsolidatedQueryResult> cqrs;
        String j;
        j = "select r "
                + " from ConsolidatedQueryResult r "
                + " where r.longValue is null "
                + " order by r.id "
                + " ";
        cqrs = getConsolidatedQueryResultFacade().findByJpql(j, singleProcessCount);

        if (cqrs == null) {
            if (logActivity) {
                System.out.println("No consolidated queries to get results from individual queries.");
            }
            return;
        }

        Map m;
        if (logActivity) {
            System.out.println("Number of consolidated queries to get results from individual queries this time is " + cqrs.size());
        }
        for (ConsolidatedQueryResult r : cqrs) {

            if (r.getLastIndividualQueryResultId() == null || r.getLastIndividualQueryResultId() == 0l) {
                r.setLongValue(0l);
                getConsolidatedQueryResultFacade().edit(r);
                continue;
            }

            m = new HashMap();
            m.put("id", r.getLastIndividualQueryResultId());
            j = "select r "
                    + " from IndividualQueryResult r "
                    + " where r.id=:id";
            IndividualQueryResult lastIndividualResult
                    = getIndividualQueryResultFacade().findFirstByJpql(j, m);
            if (lastIndividualResult == null) {
                continue;
            }
            List<Long> encIds = findEncounterIds(r.getResultFrom(), r.getResultTo(), r.getInstitution());

            if (lastIndividualResult.getIncluded() != null) {
                if (encIds == null) {
                    r.setLongValue(0l);
                    getConsolidatedQueryResultFacade().edit(r);
                    continue;
                }
                if (encIds.isEmpty()) {
                    r.setLongValue(0l);
                    getConsolidatedQueryResultFacade().edit(r);
                    continue;
                }
                for (int i = 0; i < 1000; i++) {
                    if (i % 1000 == 1) {
                        System.out.print(".");
                    }
                }
                consolideIndividualResults(r, encIds);
                for (int i = 0; i < 1000; i++) {
                    if (i % 1000 == 1) {
                        System.out.print(".");
                    }
                }
            }
        }
    }

    private void consolideIndividualResults(ConsolidatedQueryResult cr, List<Long> encIds) {
        if (logActivity) {
            System.out.println("consolide Individual Results");
        }
        String j;
        Map m;
        Long count = 0l;
        for (Long id : encIds) {
            m = new HashMap();
            j = "select r "
                    + " from IndividualQueryResult r "
                    + " where r.encounterId=:encId "
                    + " and r.queryComponentCode=:code ";
            m.put("encId", id);
            m.put("code", cr.getQueryComponentCode());
//            if(logActivity) System.out.println("j = " + j);
//            if(logActivity) System.out.println("m = " + m);
            IndividualQueryResult r = getIndividualQueryResultFacade().findFirstByJpql(j, m);
//            if(logActivity) System.out.println("r = " + r);
            if (r != null) {
                if (r.getIncluded()) {
                    count++;
                }
            }
        }
//        if(logActivity) System.out.println("count = " + count);
        cr.setLongValue(count);
        getConsolidatedQueryResultFacade().edit(cr);
    }

    private void submitToConsilidate() {
        int processingCount = 5;
        String j;
        Map m = new HashMap();
        j = "select q from StoredQueryResult q "
                + " where q.retired=false "
                + " and q.processFailed=false "
                + " and q.submittedForConsolidation=false "
                + " and q.readyAfterConsolidation=false "
                + " and q.processCompleted=false "
                + " and q.processStarted=false "
                + " order by q.id";
        List<StoredQueryResult> qs = getStoreQueryResultFacade().findByJpql(j, processingCount);

        if (qs == null) {
            if (logActivity) {
                System.out.println("No Stored Queries to submit to consolide.");
            }
            return;
        }

        if (logActivity) {
            System.out.println("Number of Stored Queries to submit to consolide is " + qs.size());
        }

        for (StoredQueryResult q : qs) {
            q.setProcessStarted(true);
            q.setProcessStartedAt(new Date());
            q.setProcessFailed(false);
            q.setProcessCompleted(false);
            getStoreQueryResultFacade().edit(q);
            boolean processSuccess = submitRecordToConsolidation(q);

            if (processSuccess) {
                q.setSubmittedForConsolidation(true);
                q.setSubmittedForConsolidationAt(new Date());
                getStoreQueryResultFacade().edit(q);
            } else {
                q.setProcessFailed(true);
                q.setProcessFailedAt(new Date());
                getStoreQueryResultFacade().edit(q);
            }
        }

    }

    private void checkCompletenessAfterConsolidation() {
        int processingCount = 5;

        String j;
        Map m = new HashMap();
        j = "select q from StoredQueryResult q "
                + " where q.retired=false "
                + " and q.processFailed=false "
                + " and q.submittedForConsolidation=true "
                + " and q.readyAfterConsolidation=false "
                + " order by q.id";
        List<StoredQueryResult> qs = getStoreQueryResultFacade().findByJpql(j, processingCount);
        if (qs == null) {
            if (logActivity) {
                System.out.println("No stored queries to check for completeness.");
            }

            return;
        }

        if (logActivity) {
            System.out.println("Number of stored queries to check for completeness is " + qs.size());
        }

        for (StoredQueryResult q : qs) {
            q.setProcessStarted(true);
            q.setProcessStartedAt(new Date());
            q.setProcessFailed(false);
            q.setProcessCompleted(false);
            getStoreQueryResultFacade().edit(q);
            boolean processSuccess = checkRecordCompletenessAfterConsolidation(q);

            if (processSuccess) {
                q.setReadyAfterConsolidation(true);
                q.setReadyAfterConsolidationAt(new Date());
                getStoreQueryResultFacade().edit(q);
            } else {
                q.setReadyAfterConsolidation(false);
                getStoreQueryResultFacade().edit(q);
            }
        }

    }

    private void generateFileAfterConsolidation() {
        int processCount = 5;

        String j;
        Map m = new HashMap();
        j = "select q from StoredQueryResult q "
                + " where q.retired=false "
                + " and q.processFailed=false "
                + " and q.submittedForConsolidation=true "
                + " and q.readyAfterConsolidation=true "
                + " and q.processCompleted=false "
                + " order by q.id";
        List<StoredQueryResult> qs = getStoreQueryResultFacade().findByJpql(j, processCount);
        if (qs == null) {

            if (logActivity) {
                System.out.println("No Stored Queries to generate files.");
            }
            return;
        }

        if (logActivity) {
            System.out.println("Number of Stored Queries to generate files is " + qs.size());
        }

        for (StoredQueryResult q : qs) {
            q.setProcessStarted(true);
            q.setProcessStartedAt(new Date());
            q.setProcessFailed(false);
            q.setProcessCompleted(false);
            getStoreQueryResultFacade().edit(q);
            boolean processSuccess = generateRecordFileAfterConsolidation(q);

            if (processSuccess) {
                q.setProcessCompleted(true);
                q.setProcessCompletedAt(new Date());
                getStoreQueryResultFacade().edit(q);
            } else {
                q.setProcessFailed(true);
                q.setProcessFailedAt(new Date());
                getStoreQueryResultFacade().edit(q);
            }
        }

    }

    private boolean submitRecordToConsolidation(StoredQueryResult sqr) {
        boolean success = false;

        QueryComponent queryComponent = sqr.getQueryComponent();
        Institution ins = sqr.getInstitution();
        ReportTimePeriod rtp = new ReportTimePeriod();
        rtp.setTimePeriodType(sqr.getTimePeriodType());
        rtp.setFrom(sqr.getResultFrom());
        rtp.setTo(sqr.getResultTo());
        rtp.setYear(sqr.getResultYear());
        rtp.setMonth(sqr.getResultMonth());
        rtp.setQuarter(sqr.getResultQuarter());
        rtp.setDateOfMonth(sqr.getResultDateOfMonth());

        if (queryComponent == null) {
            sqr.setErrorMessage("No report available.");
            getStoreQueryResultFacade().edit(sqr);
            return success;
        }

        if (queryComponent.getQueryType() == null) {
            sqr.setErrorMessage("No query type specified.");
            getStoreQueryResultFacade().edit(sqr);
            return success;
        }

        String j = "select u from Upload u "
                + " where u.component=:c";
        Map m = new HashMap();
        m.put("c", queryComponent);

        Upload upload = getUploadFacade().findFirstByJpql(j, m);
        if (upload == null) {
            sqr.setErrorMessage("No excel template uploaded.");
            getStoreQueryResultFacade().edit(sqr);
            return success;
        }

        List<QueryComponent> queryComponentsInExcelFile = new ArrayList<>();

        String FILE_NAME = upload.getFileName() + "_" + (new Date()) + ".xlsx";

        String folder = "/tmp/";

        File newFile = new File(folder + FILE_NAME);

        try {
            FileUtils.writeByteArrayToFile(newFile, upload.getBaImage());
        } catch (IOException ex) {
            sqr.setErrorMessage("IO Exception. " + ex.getMessage());
            getStoreQueryResultFacade().edit(sqr);
        }

        XSSFWorkbook workbook;
        XSSFSheet sheet;

        try {

            FileInputStream excelFile = new FileInputStream(newFile);
            workbook = new XSSFWorkbook(excelFile);
            sheet = workbook.getSheetAt(0);
            Iterator<Row> iterator = sheet.iterator();

            while (iterator.hasNext()) {

                Row currentRow = iterator.next();

                Iterator<Cell> cellIterator = currentRow.iterator();
                while (cellIterator.hasNext()) {
                    Cell currentCell = cellIterator.next();

                    String cellString = "";

                    CellType ct = currentCell.getCellType();

                    if (ct == null) {
                        continue;
                    }

                    switch (ct) {
                        case STRING:
                            cellString = currentCell.getStringCellValue();
                            break;
                        case BLANK:
                        case BOOLEAN:
                        case ERROR:
                        case FORMULA:
                        case NUMERIC:
                        case _NONE:

                            continue;
                    }

                    if (cellString.contains("#{")) {
                        QueryComponent qc = findQueryComponentByCellString(cellString);
                        if (qc != null) {
                            queryComponentsInExcelFile.add(qc);
                        }
                    }

                }

            }

            excelFile.close();

        } catch (FileNotFoundException e) {
            sqr.setErrorMessage("IO Exception. " + e.getMessage());
            getStoreQueryResultFacade().edit(sqr);
            return success;
        } catch (IOException e) {
            sqr.setErrorMessage("IO Exception. " + e.getMessage());
            getStoreQueryResultFacade().edit(sqr);
            return success;
        }

        for (QueryComponent qc : queryComponentsInExcelFile) {
            createConsolidatedQueryResult(sqr, qc);
        }

        success = true;
        return success;

    }

    private void createConsolidatedQueryResult(StoredQueryResult sqr, QueryComponent qc) {
        String j;
        Map m = new HashMap();
        j = "select r "
                + " from ConsolidatedQueryResult r "
                + " where r.resultFrom=:fd "
                + " and r.resultTo=:td "
                + " and lower(r.queryComponentCode)=:qcc "
                + " ";
        m.put("fd", sqr.getResultFrom());
        m.put("td", sqr.getResultTo());
        m.put("qcc", qc.getCode().trim().toLowerCase());
        if (sqr.getInstitution() != null) {
            j += " and r.institution=:ins ";
            m.put("ins", sqr.getInstitution());
        }
        if (sqr.getArea() != null) {
            j += " and r.area=:area ";
            m.put("area", sqr.getArea());
        }
        ConsolidatedQueryResult r = getConsolidatedQueryResultFacade().findFirstByJpql(j, m);

        if (r == null) {
            r = new ConsolidatedQueryResult();
            r.setResultFrom(sqr.getResultFrom());
            r.setResultTo(sqr.getResultTo());
            r.setQueryComponentCode(qc.getCode());
            r.setQueryComponentId(qc.getId());
            r.setInstitution(sqr.getInstitution());
            r.setArea(sqr.getArea());
            getConsolidatedQueryResultFacade().create(r);
        }

    }

    private boolean checkConsolidatedQueryResult(StoredQueryResult sqr, QueryComponent qc) {
        String j;
        Map m = new HashMap();
        j = "select r "
                + " from ConsolidatedQueryResult r "
                + " where r.resultFrom=:fd "
                + " and r.resultTo=:td "
                + " and lower(r.queryComponentCode)=:qcc "
                + " ";
        m.put("fd", sqr.getResultFrom());
        m.put("td", sqr.getResultTo());
        m.put("qcc", qc.getCode().trim().toLowerCase());
        if (sqr.getInstitution() != null) {
            j += " and r.institution=:ins ";
            m.put("ins", sqr.getInstitution());
        }
        if (sqr.getArea() != null) {
            j += " and r.area=:area ";
            m.put("area", sqr.getArea());
        }
        ConsolidatedQueryResult r = getConsolidatedQueryResultFacade().findFirstByJpql(j, m);
        boolean resultFound = true;
        if (r == null) {
            resultFound = true;
        } else {
            if (r.getLongValue() == null) {
                resultFound = false;
            } else {
                resultFound = true;
            }
        }

        return resultFound;
    }

    private Long findConsolidatedQueryResult(StoredQueryResult sqr, QueryComponent qc) {
        String j;
        Map m = new HashMap();
        j = "select r "
                + " from ConsolidatedQueryResult r "
                + " where r.resultFrom=:fd "
                + " and r.resultTo=:td "
                + " and lower(r.queryComponentCode)=:qcc "
                + " ";
        m.put("fd", sqr.getResultFrom());
        m.put("td", sqr.getResultTo());
        m.put("qcc", qc.getCode().trim().toLowerCase());
        if (sqr.getInstitution() != null) {
            j += " and r.institution=:ins ";
            m.put("ins", sqr.getInstitution());
        }
        if (sqr.getArea() != null) {
            j += " and r.area=:area ";
            m.put("area", sqr.getArea());
        }
        ConsolidatedQueryResult r = getConsolidatedQueryResultFacade().findFirstByJpql(j, m);

        if (r == null) {
            return null;
        }
        if (r.getLongValue() == null) {
            return null;
        }
        return r.getLongValue();
    }

    private Long createIndividualQueryResultsForConsolidateQueryResult(Long encounterId,
            String qryCode,
            Long queryId) {
        if (qryCode == null) {
            return 0l;
        }
        if (encounterId == null) {
            return 0l;
        }
        String j;
        Map m = new HashMap();
        j = "select r "
                + " from IndividualQueryResult r "
                + " where r.encounterId=:enid "
                + " and lower(r.queryComponentCode)=:qcc "
                + " ";
        m.put("enid", encounterId);
        m.put("qcc", qryCode.trim().toLowerCase());

        IndividualQueryResult r = getIndividualQueryResultFacade().findFirstByJpql(j, m);

        if (r == null) {
            r = new IndividualQueryResult();
            r.setEncounterId(encounterId);
            r.setQueryComponentCode(qryCode.trim().toLowerCase());
            r.setQueryComponentId(queryId);
            getIndividualQueryResultFacade().create(r);
            for (int i = 0; i < 1000; i++) {
                if (i % 1000 == 1) {
                    System.out.print(".");
                }
            }
        }

        return r.getId();
    }

    private void calculateIndividualQueryResult(IndividualQueryResult r) {
        if (logActivity) {
            System.out.println("Calculating Individual Query Results for Query " + r.getQueryComponentCode()
                    + " and Encounter " + r.getEncounterId());
        }

        for (int i = 0; i < 1000; i++) {
            if (i % 1000 == 1) {
                System.out.print(".");
            }
        }

        List<QueryComponent> criteria = findCriteriaForQueryComponent(r.getQueryComponentCode());

        for (int i = 0; i < 1000; i++) {
            if (i % 1000 == 1) {
                System.out.print(".");
            }
        }

        if (logActivity) {
            System.out.println("criteria = " + criteria);
        }
        if (criteria == null || criteria.isEmpty()) {
            r.setIncluded(true);
        } else {
            List<ClientEncounterComponentBasicDataToQuery> encomps
                    = findClientEncounterComponentItems(r.getEncounterId(), criteria);
            r.setIncluded(findMatch(encomps, criteria));
        }
        getIndividualQueryResultFacade().edit(r);
    }

//    private void calculateIndividualQueryResultAlt(IndividualQueryResult r) {
//        List<ClientEncounterComponentBasicDataToQuery> encomps
//                = findClientEncounterComponentItems(r.getEncounterId(), r.getQueryComponentId());
//        List<QueryComponent> criteria = findCriteriaForQueryComponent(r.getQueryComponentCode());
//        if (criteria == null || criteria.isEmpty()) {
//            r.setIncluded(true);
//        } else {
//            r.setIncluded(findMatch(encomps, criteria));
//        }
//        getIndividualQueryResultFacade().edit(r);
//    }
    private boolean findMatch(List<ClientEncounterComponentBasicDataToQuery> is, List<QueryComponent> qrys) {
        boolean suitableForInclusion = true;
        for (QueryComponent q : qrys) {

            if (q.getItem() == null || q.getItem().getCode() == null) {
                continue;
            } else {
//                    if(logActivity) System.out.println("QUERY Item Code is NULL");
            }

            boolean thisMatchOk = false;
            boolean componentFound = false;
            for (ClientEncounterComponentBasicDataToQuery i : is) {

                if (i.getItemCode() == null) {
                    continue;
                }

                if (i.getItemCode().trim().equalsIgnoreCase(q.getItem().getCode().trim())) {
                    componentFound = true;
                    if (matchQuery(q, i)) {
                        thisMatchOk = true;
                    }
                }
            }
            if (!componentFound) {
                if (logActivity) {
                    System.out.println("Client component Item NOT found for " + q.getItem().getCode());
                    for (ClientEncounterComponentBasicDataToQuery ci : is) {
                        System.out.println("Client Component Item Item Code = " + ci.getItemCode());
                    }
                    for (QueryComponent qc : qrys) {
                        System.out.println("qc Item Code " + qc.getItem().getCode());
                    }
                }
            }
            if (!thisMatchOk) {
                suitableForInclusion = false;
            }
        }
        return suitableForInclusion;
    }

    private boolean checkRecordCompletenessAfterConsolidation(StoredQueryResult sqr) {
//        if(logActivity) System.out.println("checkRecordCompletenessAfterConsolidation");
        boolean success = false;

        QueryComponent queryComponent = sqr.getQueryComponent();
        Institution ins = sqr.getInstitution();
        ReportTimePeriod rtp = new ReportTimePeriod();
        rtp.setTimePeriodType(sqr.getTimePeriodType());
        rtp.setFrom(sqr.getResultFrom());
        rtp.setTo(sqr.getResultTo());
        rtp.setYear(sqr.getResultYear());
        rtp.setMonth(sqr.getResultMonth());
        rtp.setQuarter(sqr.getResultQuarter());
        rtp.setDateOfMonth(sqr.getResultDateOfMonth());

        if (queryComponent == null) {
            sqr.setErrorMessage("No report available.");
            getStoreQueryResultFacade().edit(sqr);
            return success;
        }

        if (queryComponent.getQueryType() == null) {
            sqr.setErrorMessage("No query type specified.");
            getStoreQueryResultFacade().edit(sqr);
            return success;
        }

        String j = "select u from Upload u "
                + " where u.component=:c";
        Map m = new HashMap();
        m.put("c", queryComponent);

        Upload upload = getUploadFacade().findFirstByJpql(j, m);
        if (upload == null) {
            sqr.setErrorMessage("No excel template uploaded.");
            getStoreQueryResultFacade().edit(sqr);
            return success;
        }

        List<QueryComponent> queryComponentsInExcelFile = new ArrayList<>();

        String FILE_NAME = upload.getFileName() + "_" + (new Date()) + ".xlsx";

        String folder = "/tmp/";

        File newFile = new File(folder + FILE_NAME);

        try {
            FileUtils.writeByteArrayToFile(newFile, upload.getBaImage());
        } catch (IOException ex) {
            sqr.setErrorMessage("IO Exception. " + ex.getMessage());
            getStoreQueryResultFacade().edit(sqr);
        }

        XSSFWorkbook workbook;
        XSSFSheet sheet;

        try {

            FileInputStream excelFile = new FileInputStream(newFile);
            workbook = new XSSFWorkbook(excelFile);
            sheet = workbook.getSheetAt(0);
            Iterator<Row> iterator = sheet.iterator();

            while (iterator.hasNext()) {

                Row currentRow = iterator.next();

                Iterator<Cell> cellIterator = currentRow.iterator();
                while (cellIterator.hasNext()) {
                    Cell currentCell = cellIterator.next();

                    String cellString = "";

                    CellType ct = currentCell.getCellType();

                    if (ct == null) {
                        continue;
                    }

                    switch (ct) {
                        case STRING:
                            cellString = currentCell.getStringCellValue();
                            break;
                        case BLANK:
                        case BOOLEAN:
                        case ERROR:
                        case FORMULA:
                        case NUMERIC:
                        case _NONE:

                            continue;
                    }

                    if (cellString.contains("#{")) {
                        QueryComponent qc = findQueryComponentByCellString(cellString);
                        if (qc != null) {
                            queryComponentsInExcelFile.add(qc);
                        }
                    }

                }

            }

            excelFile.close();

        } catch (FileNotFoundException e) {
            sqr.setErrorMessage("IO Exception. " + e.getMessage());
            getStoreQueryResultFacade().edit(sqr);
            return success;
        } catch (IOException e) {
            sqr.setErrorMessage("IO Exception. " + e.getMessage());
            getStoreQueryResultFacade().edit(sqr);
            return success;
        }

        success = true;
        for (QueryComponent qc : queryComponentsInExcelFile) {
            if (checkConsolidatedQueryResult(sqr, qc) == false) {
                success = false;
            }
        }

        return success;

    }

    private boolean generateRecordFileAfterConsolidation(StoredQueryResult sqr) {
        boolean success = false;

        QueryComponent queryComponent = sqr.getQueryComponent();
        Institution ins = sqr.getInstitution();
        ReportTimePeriod rtp = new ReportTimePeriod();
        rtp.setTimePeriodType(sqr.getTimePeriodType());
        rtp.setFrom(sqr.getResultFrom());
        rtp.setTo(sqr.getResultTo());
        rtp.setYear(sqr.getResultYear());
        rtp.setMonth(sqr.getResultMonth());
        rtp.setQuarter(sqr.getResultQuarter());
        rtp.setDateOfMonth(sqr.getResultDateOfMonth());

        if (queryComponent == null) {
            sqr.setErrorMessage("No report available.");
            getStoreQueryResultFacade().edit(sqr);
            return success;
        }

        if (queryComponent.getQueryType() == null) {
            sqr.setErrorMessage("No query type specified.");
            getStoreQueryResultFacade().edit(sqr);
            return success;
        }

        String j = "select u from Upload u "
                + " where u.component=:c";
        Map m = new HashMap();
        m.put("c", queryComponent);

        Upload upload = getUploadFacade().findFirstByJpql(j, m);
        if (upload == null) {
            sqr.setErrorMessage("No excel template uploaded.");
            getStoreQueryResultFacade().edit(sqr);
            return success;
        }

        String FILE_NAME = upload.getFileName() + "_" + (new Date()) + ".xlsx";

        String folder = "/tmp/";

        File newFile = new File(folder + FILE_NAME);

        try {
            FileUtils.writeByteArrayToFile(newFile, upload.getBaImage());
        } catch (IOException ex) {
            sqr.setErrorMessage("IO Exception. " + ex.getMessage());
            getStoreQueryResultFacade().edit(sqr);
        }

        XSSFWorkbook workbook;
        XSSFSheet sheet;

        try {

            FileInputStream excelFile = new FileInputStream(newFile);
            workbook = new XSSFWorkbook(excelFile);
            sheet = workbook.getSheetAt(0);
            Iterator<Row> iterator = sheet.iterator();

            while (iterator.hasNext()) {

                Row currentRow = iterator.next();

                Iterator<Cell> cellIterator = currentRow.iterator();
                while (cellIterator.hasNext()) {
                    Cell currentCell = cellIterator.next();

                    String cellString = "";

                    CellType ct = currentCell.getCellType();

                    if (ct == null) {
                        continue;
                    }

                    switch (ct) {
                        case STRING:
                            cellString = currentCell.getStringCellValue();
                            break;
                        case BLANK:
                        case BOOLEAN:
                        case ERROR:
                        case FORMULA:
                        case NUMERIC:
                        case _NONE:

                            continue;
                    }

                    if (cellString.contains("#{")) {
                        if (cellString.equals("#{report_institute}")) {
                            if (sqr.getInstitution() != null) {
                                currentCell.setCellValue(sqr.getInstitution().getName());
                            }
                        } else if (cellString.equals("#{report_period}")) {
                            currentCell.setCellValue(sqr.getPeriodString());
                        } else {
                            QueryComponent qc = findQueryComponentByCellString(cellString);
                            if (qc != null) {
                                Long value = findConsolidatedQueryResult(sqr, qc);
                                if (value != null) {
                                    currentCell.setCellValue(value);
                                }
                            }
                        }
                    }

                }

            }

            excelFile.close();
            FileOutputStream out = new FileOutputStream(FILE_NAME);
            workbook.write(out);
            out.close();

////                if(logActivity) System.out.println("FILE_NAME = " + FILE_NAME);
            InputStream stream;
            stream = new FileInputStream(FILE_NAME);

            Upload u = new Upload();
            u.setFileName(FILE_NAME);
            u.setFileType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
            u.setCreatedAt(new Date());

            getUploadFacade().create(u);

            byte[] byteArray = IOUtils.toByteArray(stream);
            u.setBaImage(byteArray);

//                if(logActivity) System.out.println("5 = " + 5);
            sqr.setUpload(u);
            getStoreQueryResultFacade().edit(sqr);

        } catch (FileNotFoundException e) {
            sqr.setErrorMessage("IO Exception. " + e.getMessage());
            getStoreQueryResultFacade().edit(sqr);
            return success;
        } catch (IOException e) {
            sqr.setErrorMessage("IO Exception. " + e.getMessage());
            getStoreQueryResultFacade().edit(sqr);
            return success;
        }

        success = true;
        return success;

    }

    private QueryComponent findQueryComponentByCellString(String text) {
        String str;

        String patternStart = "#{";
        String patternEnd = "}";
        String regexString = Pattern.quote(patternStart) + "(.*?)" + Pattern.quote(patternEnd);

        Pattern p = Pattern.compile(regexString);

        Matcher m = p.matcher(text);
        QueryComponent qc = null;
        while (m.find()) {
            String block = m.group(1);
            str = block;
            QueryComponent tqc = findQueryComponentByCode(block);
            if (tqc != null) {
                qc = tqc;
            }
        }
        return qc;
    }

    private QueryComponent findQueryComponentByCode(String code) {
        if (code == null) {
            return null;
        }
        for (QueryComponent qc : getQueryComponents()) {
            if (qc.getCode() == null) {
                continue;
            }
            if (qc.getCode().trim().equals(code.trim())) {
                return qc;
            }
        }
        return null;
    }

//    private Long findReplaceblesInCalculationString(String text, List<EncounterWithComponents> ens) {
//        String str;
//        Long l = 0l;
//
//        if (ens == null) {
//            return l;
//        }
//        if (ens.isEmpty()) {
//            l = 0l;
//            return l;
//        }
//
//        String patternStart = "#{";
//        String patternEnd = "}";
//        String regexString = Pattern.quote(patternStart) + "(.*?)" + Pattern.quote(patternEnd);
//
//        Pattern p = Pattern.compile(regexString);
//
//        Matcher m = p.matcher(text);
//
//        while (m.find()) {
//            String block = m.group(1);
//            str = block;
//            QueryComponent qc = findQueryComponentByCode(block);
//            if (qc == null) {
//                str += " not qc";
//                l = null;
////                if(logActivity) System.out.println("l = " + l);
//                return l;
//
//            } else {
////                if(logActivity) System.out.println("qc = " + qc.getName());
//                if (qc.getQueryType() == QueryType.Encounter_Count) {
//                    List<QueryComponent> criteria = findCriteriaForQueryComponent(qc);
//
//                    if (criteria == null || criteria.isEmpty()) {
//                        l = Long.valueOf(ens.size());
//                        str += " " + l;
//                        return l;
//                    } else {
//                        l = findMatchingCount(ens, criteria);
//                        str += criteria + " " + l;
//                        return l;
//                    }
//
//                } else {
////                    str += " not encounter count";
//                    l = null;
//                    return l;
//                }
//            }
//
//        }
//
//        return l;
//
//    }
    private List<Long> findEncounterIds(Date fromDate, Date toDate, Institution institution) {
        String j = "select e.id "
                + " from  ClientEncounterComponentFormSet f join f.encounter e"
                + " where e.retired<>:er"
                + " and f.retired<>:fr ";
        j += " and f.completed=:fc ";
        j += " and e.institution=:i "
                + " and e.encounterType=:t "
                + " and e.encounterDate between :fd and :td"
                + " order by e.id";
        Map m = new HashMap();
        m.put("i", institution);
        m.put("t", EncounterType.Clinic_Visit);
        m.put("er", true);
        m.put("fr", true);
        m.put("fc", true);
        m.put("fd", fromDate);
        m.put("td", toDate);
        List<Long> encs = encounterFacade.findLongList(j, m);
        return encs;
    }

//    private Long findMatchingCount(List<EncounterWithComponents> encs, List<QueryComponent> qrys) {
//        Long c = 0l;
//        Long encounterCount = 1l;
//        for (EncounterWithComponents e : encs) {
//            List<ClientEncounterComponentBasicDataToQuery> is = e.getComponents();
//            boolean suitableForInclusion = true;
//            for (QueryComponent q : qrys) {
//
//                if (q.getItem() == null || q.getItem().getCode() == null) {
//                    continue;
//                } else {
////                    if(logActivity) System.out.println("QUERY Item Code is NULL");
//                }
//
//                boolean thisMatchOk = false;
//                boolean componentFound = false;
//                for (ClientEncounterComponentBasicDataToQuery i : is) {
//
//                    if (i.getItemCode() == null) {
//                        continue;
//                    }
//
//                    if (i.getItemCode().trim().equalsIgnoreCase(q.getItem().getCode().trim())) {
//                        componentFound = true;
//                        if (matchQuery(q, i)) {
//                            thisMatchOk = true;
////                            if(logActivity) System.out.println("i.getItemCode() = " + i.getItemCode());
////                            if(logActivity) System.out.println("q.getItem().getCode() = " + q.getItem().getCode());
////                            if(logActivity) System.out.println("thisMatchOk = " + thisMatchOk);
//                        }
//                    }
//                }
//                if (!componentFound) {
////                    if(logActivity) System.out.println("Client component Item NOT found for " + q.getItem().getCode());
//                }
//                if (!thisMatchOk) {
//                    suitableForInclusion = false;
//                }
//            }
//            if (suitableForInclusion) {
//                c++;
//            }
//            encounterCount++;
//        }
//        return c;
//    }
    private boolean matchQuery(QueryComponent q, ClientEncounterComponentBasicDataToQuery clientValue) {
        if (clientValue == null) {
            return false;
        }
        boolean m = false;
        Integer qInt1 = null;
        Integer qInt2 = null;
        Double real1 = null;
        Double real2 = null;
        Long lng1 = null;
        Long lng2 = null;
        Item itemVariable = null;
        Item itemValue = null;
        Boolean qBool = null;

        if (q.getMatchType() == QueryCriteriaMatchType.Variable_Value_Check) {
//            if(logActivity) System.out.println("q.getQueryDataType() = " + q.getQueryDataType());
            switch (q.getQueryDataType()) {
                case integer:
//                    if(logActivity) System.out.println("clientValue.getIntegerNumberValue() = " + clientValue.getIntegerNumberValue());

                    qInt1 = q.getIntegerNumberValue();
                    qInt2 = q.getIntegerNumberValue2();
//                    if(logActivity) System.out.println("Query int1 = " + qInt1);
//                    if(logActivity) System.out.println("Query int2 = " + qInt2);
                    break;
                case item:
//                    if(logActivity) System.out.println("clientValue.getItemCode() = " + clientValue.getItemCode());
//                    if(logActivity) System.out.println("clientValue.getItemValueCode() = " + clientValue.getItemValueCode());

                    itemValue = q.getItemValue();
                    itemVariable = q.getItem();
                    break;
                case real:
//                    if(logActivity) System.out.println("clientValue.getRealNumberValue() = " + clientValue.getRealNumberValue());

                    real1 = q.getRealNumberValue();
                    real2 = q.getRealNumberValue2();
                    break;
                case longNumber:
                    lng1 = q.getLongNumberValue();
                    lng2 = q.getLongNumberValue2();
                    break;
                case Boolean:
                    qBool = q.getBooleanValue();
                    break;

            }
//            if(logActivity) System.out.println("q.getEvaluationType() = " + q.getEvaluationType());
            switch (q.getEvaluationType()) {

                case Not_null:
                    if (qInt1 != null) {
                        Integer tmpIntVal = clientValue.getIntegerNumberValue();
                        if (tmpIntVal == null) {
                            tmpIntVal = stringToInteger(clientValue.getShortTextValue());
                        }
                        if (tmpIntVal != null) {
                            m = true;
                        }
                    }
                    if (lng1 != null) {
                        Long tmpLLongVal = clientValue.getLongNumberValue();
                        if (tmpLLongVal == null) {
                            tmpLLongVal = stringToLong(clientValue.getShortTextValue());
                        }
                        if (tmpLLongVal != null) {
                            m = true;
                        }
                    }
                    if (real1 != null) {
                        Double tmpDbl = clientValue.getRealNumberValue();
                        if (tmpDbl == null) {
                            tmpDbl = stringToDouble(clientValue.getShortTextValue());
                        }
                        if (tmpDbl != null) {
                            m = true;
                        }
                    }
                    if (qBool != null) {
                        if (clientValue.getBooleanValue() != null) {
                            m = true;
                        }
                    }
                    if (itemValue != null && itemVariable != null) {
                        if (itemValue.getCode() != null) {
                            if (clientValue.getItemValueCode() != null) {
                                m = true;
                            }
                        }
                    }
                    break;

                case Equal:
                    if (qInt1 != null) {
                        Integer tmpIntVal = clientValue.getIntegerNumberValue();
                        if (tmpIntVal == null) {
                            tmpIntVal = stringToInteger(clientValue.getShortTextValue());
                        }
                        if (tmpIntVal != null) {
                            m = qInt1.equals(tmpIntVal);
                        }
                    }
                    if (lng1 != null) {
                        Long tmpLLongVal = clientValue.getLongNumberValue();
                        if (tmpLLongVal == null) {
                            tmpLLongVal = stringToLong(clientValue.getShortTextValue());
                        }
                        if (tmpLLongVal != null) {
                            m = lng1.equals(tmpLLongVal);
                        }
                    }
                    if (real1 != null) {
                        Double tmpDbl = clientValue.getRealNumberValue();
                        if (tmpDbl == null) {
                            tmpDbl = stringToDouble(clientValue.getShortTextValue());
                        }
                        if (tmpDbl != null) {
                            m = real1.equals(tmpDbl);
                        }
                    }
                    if (qBool != null) {
                        if (clientValue.getBooleanValue() != null) {
                            m = qBool.equals(clientValue.getBooleanValue());
                        }
                    }
                    if (itemValue != null && itemVariable != null) {
                        if (itemValue.getCode() != null
                                && clientValue.getItemValueCode() != null) {

                            if (itemValue.getCode().equals(clientValue.getItemValueCode())) {
                                m = true;
                            }
                        }
                    }
                    break;
                case Less_than:
                    if (qInt1 != null) {
                        Integer tmpIntVal = clientValue.getIntegerNumberValue();
//                        if(logActivity) System.out.println("1 tmpIntVal = " + tmpIntVal);
                        if (tmpIntVal == null) {
//                            if(logActivity) System.out.println("clientValue.getShortTextValue() = " + clientValue.getShortTextValue());
                            tmpIntVal = stringToInteger(clientValue.getShortTextValue());
//                            if(logActivity) System.out.println("2 tmpIntVal = " + tmpIntVal);
                        }
                        if (tmpIntVal != null) {
//                            if(logActivity) System.out.println("qInt1 = " + qInt1);

                            m = tmpIntVal < qInt1;
//                            if(logActivity) System.out.println("m = " + m);
                        }
                    }
                    if (lng1 != null) {
                        Long tmpLong = clientValue.getLongNumberValue();
                        if (tmpLong == null) {
                            tmpLong = stringToLong(clientValue.getShortTextValue());
                        }
                        if (tmpLong != null) {
                            m = tmpLong < lng1;
                        }
                    }
                    if (real1 != null) {
                        Double tmpDbl = clientValue.getRealNumberValue();
                        if (tmpDbl == null) {
                            tmpDbl = stringToDouble(clientValue.getShortTextValue());
                        }
                        if (tmpDbl != null) {
                            m = tmpDbl < real1;
                        }
                    }
                    break;
                case Between:
                    if (qInt1 != null && qInt2 != null) {
                        if (qInt1 > qInt2) {
                            Integer intTem = qInt1;
                            qInt1 = qInt2;
                            qInt2 = intTem;
                        }

                        Integer tmpInt = clientValue.getIntegerNumberValue();
                        if (tmpInt == null) {
                            tmpInt = stringToInteger(clientValue.getShortTextValue());
                        }
                        if (tmpInt != null) {
                            if (tmpInt > qInt1 && tmpInt < qInt2) {
                                m = true;
                            }
                        }

                    }
                    if (lng1 != null && lng2 != null) {
                        if (lng1 > lng2) {
                            Long intTem = lng1;
                            intTem = lng1;
                            lng1 = lng2;
                            lng2 = intTem;
                        }

                        Long tmpLong = clientValue.getLongNumberValue();
                        if (tmpLong == null) {
                            tmpLong = stringToLong(clientValue.getShortTextValue());
                        }
                        if (tmpLong != null) {
                            if (tmpLong > lng1 && tmpLong < lng2) {
                                m = true;
                            }
                        }
                    }
                    if (real1 != null && real2 != null) {
                        if (real1 > real2) {
                            Double realTem = real1;
                            realTem = real1;
                            real1 = real2;
                            real2 = realTem;
                        }

                        Double tmpDbl = clientValue.getRealNumberValue();
                        if (tmpDbl == null) {
                            tmpDbl = stringToDouble(clientValue.getShortTextValue());
                        }
                        if (tmpDbl != null) {
                            if (tmpDbl > real1 && tmpDbl < real2) {
                                m = true;
                            }
                        }
                    }
                    break;
                case Grater_than:
                    if (qInt1 != null) {
                        Integer tmpInt = clientValue.getIntegerNumberValue();
                        if (tmpInt == null) {
                            tmpInt = stringToInteger(clientValue.getShortTextValue());
                        }
                        if (tmpInt != null) {
                            m = tmpInt > qInt1;
                        }
                    }
                    if (real1 != null) {
                        Double tmpDbl = clientValue.getRealNumberValue();
                        if (tmpDbl == null) {
                            tmpDbl = stringToDouble(clientValue.getShortTextValue());
                        }
                        if (tmpDbl != null) {
                            m = tmpDbl > real1;
                        }
                    }
                    if (lng1 != null) {
                        Long tmpLng = clientValue.getLongNumberValue();
                        if (tmpLng == null) {
                            tmpLng = stringToLong(clientValue.getShortTextValue());
                        }
                        if (tmpLng != null) {
                            m = tmpLng > lng1;
                        }
                    }
                    break;
                case Grater_than_or_equal:
                    if (qInt1 != null) {
                        Integer tmpInt = clientValue.getIntegerNumberValue();
                        if (tmpInt == null) {
                            tmpInt = stringToInteger(clientValue.getShortTextValue());
                        }
                        if (tmpInt != null) {
                            m = tmpInt >= qInt1;
                        }
                    }
                    if (real1 != null) {
                        Double temDbl = clientValue.getRealNumberValue();
                        if (temDbl == null) {
                            temDbl = stringToDouble(clientValue.getShortTextValue());
                        }
                        if (temDbl != null) {
                            m = temDbl >= real1;
                        }

                    }
                    if (lng1 != null) {
                        Long tmpLng = clientValue.getLongNumberValue();
                        if (tmpLng == null) {
                            tmpLng = stringToLong(clientValue.getShortTextValue());
                        }
                        if (tmpLng != null) {
                            m = tmpLng >= lng1;
                        }
                    }
                case Less_than_or_equal:
                    if (qInt1 != null) {
                        Integer tmpInt = clientValue.getIntegerNumberValue();
                        if (tmpInt == null) {
                            tmpInt = stringToInteger(clientValue.getShortTextValue());
                        }
                        if (tmpInt != null) {
                            m = tmpInt <= qInt1;
                        }
                    }
                    if (real1 != null) {
                        Double tmpDbl = clientValue.getRealNumberValue();
                        if (tmpDbl == null) {
                            tmpDbl = stringToDouble(clientValue.getShortTextValue());
                        }
                        if (tmpDbl != null) {
                            m = tmpDbl <= real1;
                        }
                    }
                    if (lng1 != null) {
                        Long tmpLng = clientValue.getLongNumberValue();
                        if (tmpLng == null) {
                            tmpLng = stringToLong(clientValue.getShortTextValue());
                        }
                        if (tmpLng != null) {
                            m = tmpLng <= lng1;
                        }
                    }
                    break;
            }
        }
        return m;
    }

    private String removeNonNumericCharactors(String str) {
        return str.replaceAll("[^\\d.]", "");
    }

    private String currentTimeAsString() {
        Date date = Calendar.getInstance().getTime();
        DateFormat dateFormat = new SimpleDateFormat("hh:mm:ss");
        String strDate = dateFormat.format(date);
        return strDate;
    }

    private Integer stringToInteger(String str) {
        Integer outInt;
        if (str == null) {
            outInt = null;
            return outInt;
        }
        str = removeNonNumericCharactors(str);

        try {
            outInt = Integer.parseInt(str);
        } catch (NumberFormatException e) {
            outInt = null;
        }
        return outInt;
    }

    private Long stringToLong(String str) {
        Long outLong;
        if (str == null) {
            outLong = null;
            return outLong;
        }
        str = removeNonNumericCharactors(str);
        try {
            outLong = Long.parseLong(str);
        } catch (NumberFormatException e) {
            outLong = null;
        }
        return outLong;
    }

    private Double stringToDouble(String str) {
        Double outDbl;
        if (str == null) {
            outDbl = null;
            return outDbl;
        }
        str = removeNonNumericCharactors(str);

        try {
            outDbl = Double.parseDouble(str);
        } catch (NumberFormatException e) {
            outDbl = null;
        }
        return outDbl;
    }

    private List<ClientEncounterComponentBasicDataToQuery> findClientEncounterComponentItems(Long endId, List<QueryComponent> qrys) {
        if (logActivity) {
            System.out.println("Finding ENcounter Component Items for Querying");
        }

        String j;
        Map m;

        List<String> tqs = new ArrayList();
        for (QueryComponent qc : qrys) {
            if (qc.getItem() != null && qc.getItem().getCode() != null) {
                if (logActivity) {
                    System.out.println("qc.getItem().getCode()  = " + qc.getItem().getCode());
                }
                tqs.add(qc.getItem().getCode().trim().toLowerCase());
            }
        }

        m = new HashMap();
        j = "select f from ClientEncounterComponentItem f "
                + " where f.retired=false "
                + " and f.encounter.id=:eid ";
        m.put("eid", endId);
//        System.out.println("tqs = " + tqs);
        if (!tqs.isEmpty()) {
//            System.out.println("tqs.size() = " + tqs.size());
            if (tqs.size() < 20) {
                j += " and lower(f.item.code) in :ics";
                m.put("ics", tqs);
            }
        } else {
            if (logActivity) {
                System.out.println("tqs = " + tqs);
            }
        }

//        System.out.println("m = " + m);
//        System.out.println("j = " + j);
        List<ClientEncounterComponentItem> t = getClientEncounterComponentItemFacade().findByJpql(j, m);
        if (t == null) {
            t = new ArrayList<>();
        }
        if (logActivity) {
            System.out.println("t = " + t.size());
        }

//        j = "select new lk.gov.health.phsp.pojcs.ClientEncounterComponentBasicDataToQuery("
//                + "f.name, "
//                + "f.code, "
//                + "f.item.code, "
//                + "f.shortTextValue, "
//                + "f.integerNumberValue, "
//                + "f.longNumberValue, "
//                + "f.realNumberValue, "
//                + "f.booleanValue, "
//                + "f.dateValue, "
//                + "f.itemValue.code"
//                + ") ";
//
//        j += " from ClientEncounterComponentItem f "
//                + " where f.retired=false "
//                + " and f.encounter.id=:eid";
//        Map m = new HashMap();
//        m.put("eid", endId);
//
//        List<Object> objs = getClientEncounterComponentItemFacade().findAggregates(j, m);
//        if(logActivity) System.out.println("objs = " + objs.size());
        List<ClientEncounterComponentBasicDataToQuery> ts = new ArrayList<>();
        for (ClientEncounterComponentItem o : t) {
            ClientEncounterComponentBasicDataToQuery cbd;
            String itemCode = "";
            String itemValueCode = "";
            if (o.getItem() != null) {
                itemCode = o.getItem().getCode();
            }
            if (o.getItemValue() != null) {
                itemValueCode = o.getItemValue().getCode();
            }

            cbd
                    = new ClientEncounterComponentBasicDataToQuery(
                            o.getName(),
                            o.getCode(),
                            itemCode,
                            o.getShortTextValue(),
                            o.getIntegerNumberValue(),
                            o.getLongNumberValue(),
                            o.getRealNumberValue(),
                            o.getBooleanValue(),
                            o.getDateValue(),
                            itemValueCode
                    );

//            if(logActivity) System.out.println("Name = " + cbd.getName());
//            if(logActivity) System.out.println("Code = " + cbd.getCode());
//            if(logActivity) System.out.println("getItemCode = " + cbd.getItemCode());
            ts.add(cbd);

        }
        if (logActivity) {
            System.out.println("ts = " + ts.size());
        }
        return ts;
    }

    private List<ClientEncounterComponentBasicDataToQuery> findClientEncounterComponentItems(Long endId, Long queryId) {
        String j;

        Map m = new HashMap();

        j = "select q.item.code from QueryComponent q "
                + " where q.queryLevel=:ql "
                + " and q.parentComponent.id=:qid";
        m.put("ql", QueryLevel.Criterian);
        m.put("qid", queryId);

        List<String> tqs = getQueryComponentFacade().findString(j, m);

        m = new HashMap();
        j = "select f from ClientEncounterComponentItem f "
                + " where f.retired=false "
                + " and f.encounter.id=:eid ";
        m.put("eid", endId);
//        System.out.println("tqs = " + tqs);
        if (tqs != null && !tqs.isEmpty()) {
//            System.out.println("tqs.size() = " + tqs.size());
            if (tqs.size() < 20) {
                j += " and f.item.code in :ics";
                m.put("ics", tqs);
            }
        } else {
            if (logActivity) {
                System.out.println("tqs = " + tqs);
            }
        }

//        System.out.println("m = " + m);
//        System.out.println("j = " + j);
        List<ClientEncounterComponentItem> t = getClientEncounterComponentItemFacade().findByJpql(j, m);
        if (t == null) {
            t = new ArrayList<>();
        }

//        j = "select new lk.gov.health.phsp.pojcs.ClientEncounterComponentBasicDataToQuery("
//                + "f.name, "
//                + "f.code, "
//                + "f.item.code, "
//                + "f.shortTextValue, "
//                + "f.integerNumberValue, "
//                + "f.longNumberValue, "
//                + "f.realNumberValue, "
//                + "f.booleanValue, "
//                + "f.dateValue, "
//                + "f.itemValue.code"
//                + ") ";
//
//        j += " from ClientEncounterComponentItem f "
//                + " where f.retired=false "
//                + " and f.encounter.id=:eid";
//        Map m = new HashMap();
//        m.put("eid", endId);
//
//        List<Object> objs = getClientEncounterComponentItemFacade().findAggregates(j, m);
//        if(logActivity) System.out.println("objs = " + objs.size());
        List<ClientEncounterComponentBasicDataToQuery> ts = new ArrayList<>();
        for (ClientEncounterComponentItem o : t) {
            ClientEncounterComponentBasicDataToQuery cbd;
            if (o.getItem() != null && o.getItemValue() != null) {
                cbd
                        = new ClientEncounterComponentBasicDataToQuery(
                                o.getName(),
                                o.getCode(),
                                o.getItem().getCode(),
                                o.getShortTextValue(),
                                o.getIntegerNumberValue(),
                                o.getLongNumberValue(),
                                o.getRealNumberValue(),
                                o.getBooleanValue(),
                                o.getDateValue(),
                                o.getItemValue().getCode()
                        );
            } else if (o.getItem() != null) {
                cbd
                        = new ClientEncounterComponentBasicDataToQuery(
                                o.getName(),
                                o.getCode(),
                                o.getItem().getCode(),
                                o.getShortTextValue(),
                                o.getIntegerNumberValue(),
                                o.getLongNumberValue(),
                                o.getRealNumberValue(),
                                o.getBooleanValue(),
                                o.getDateValue()
                        );

            } else if (o.getItemValue() != null) {
                cbd
                        = new ClientEncounterComponentBasicDataToQuery(
                                o.getName(),
                                o.getCode(),
                                o.getShortTextValue(),
                                o.getIntegerNumberValue(),
                                o.getLongNumberValue(),
                                o.getRealNumberValue(),
                                o.getBooleanValue(),
                                o.getDateValue(),
                                o.getItemValue().getCode()
                        );
            } else {
                cbd
                        = new ClientEncounterComponentBasicDataToQuery(
                                o.getName(),
                                o.getCode(),
                                o.getShortTextValue(),
                                o.getIntegerNumberValue(),
                                o.getLongNumberValue(),
                                o.getRealNumberValue(),
                                o.getBooleanValue(),
                                o.getDateValue()
                        );
            }

//            if(logActivity) System.out.println("Name = " + cbd.getName());
//            if(logActivity) System.out.println("Code = " + cbd.getCode());
//            if(logActivity) System.out.println("getItemCode = " + cbd.getItemCode());
            ts.add(cbd);
//            if(logActivity) System.out.println("ts = " + ts.size());

        }
        return ts;
    }

    private List<QueryComponent> findCriteriaForQueryComponent(QueryComponent p) {
        if (p == null) {
            return null;
        }
        List<QueryComponent> output = new ArrayList<>();
        for (QueryComponent qc : getQueryComponents()) {
            if (qc.getQueryLevel() == null) {
                continue;
            }
            if (qc.getParentComponent() == null) {
                continue;
            }

            if (qc.getQueryLevel() == QueryLevel.Criterian) {
                if (qc.getParentComponent().equals(p)) {
                    output.add(qc);
                }
            }
        }
        return output;
    }

    private List<QueryComponent> findCriteriaForQueryComponent(String qryCode) {
        if (qryCode == null) {
            return null;
        }
        List<QueryComponent> output = new ArrayList<>();
        for (QueryComponent qc : getQueryComponents()) {
            if (qc.getQueryLevel() == null) {
                continue;
            }
            if (qc.getParentComponent() == null) {
                continue;
            }

            if (qc.getQueryLevel() == QueryLevel.Criterian) {
                if (qc.getParentComponent().getCode().equalsIgnoreCase(qryCode)) {
                    output.add(qc);
                }
            }
        }
        return output;
    }

    private List<QueryComponent> findAllQueryComponents() {
        String j = "select q from QueryComponent q "
                + " where q.retired=false ";
        List<QueryComponent> c = getQueryComponentFacade().findByJpql(j);
        return c;
    }

    private StoredQueryResultFacade getStoreQueryResultFacade() {
        return storeQueryResultFacade;
    }

    private UploadFacade getUploadFacade() {
        return uploadFacade;
    }

    private EncounterFacade getEncounterFacade() {
        return encounterFacade;
    }

    private QueryComponentFacade getQueryComponentFacade() {
        return queryComponentFacade;
    }

    private ClientEncounterComponentItemFacade getClientEncounterComponentItemFacade() {
        return clientEncounterComponentItemFacade;
    }

    private List<QueryComponent> getQueryComponents() {
        if (queryComponents == null) {
            queryComponents = findAllQueryComponents();
        }

        return queryComponents;
    }

    private void setQueryComponents(List<QueryComponent> queryComponents) {
        this.queryComponents = queryComponents;
    }

    private ConsolidatedQueryResultFacade getConsolidatedQueryResultFacade() {
        return consolidatedQueryResultFacade;
    }

    private IndividualQueryResultFacade getIndividualQueryResultFacade() {
        return individualQueryResultFacade;
    }

}
