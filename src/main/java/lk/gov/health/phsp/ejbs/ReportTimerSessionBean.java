/*
 * The MIT License
 *
 * Copyright 2020 buddhika.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package lk.gov.health.phsp.ejbs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.ejb.EJB;
import javax.ejb.Schedule;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import static javax.ejb.TransactionAttributeType.REQUIRES_NEW;
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
import lk.gov.health.phsp.enums.QueryType;
import lk.gov.health.phsp.entity.StoredQueryResult;
import lk.gov.health.phsp.facade.ClientEncounterComponentItemFacade;
import lk.gov.health.phsp.facade.ConsolidatedQueryResultFacade;
import lk.gov.health.phsp.facade.EncounterFacade;
import lk.gov.health.phsp.facade.IndividualQueryResultFacade;
import lk.gov.health.phsp.facade.QueryComponentFacade;
import lk.gov.health.phsp.facade.StoredQueryResultFacade;
import lk.gov.health.phsp.facade.UploadFacade;
import lk.gov.health.phsp.pojcs.EncounterWithComponents;
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
@Stateless
public class ReportTimerSessionBean {

    private boolean processingReport = false;

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

    @Schedule(
            hour = "*",
            minute = "*",
            second = "1",
            persistent = false)
    @TransactionAttribute(REQUIRES_NEW)
    public void runReports() {
        queryComponents = null;
        System.out.println("Running Reports = " + new Date());
        if (!processingReport) {
            submitToConsilidate();
        }
        if (!processingReport) {
            checkCompletenessAfterConsolidation();
        }
        if (!processingReport) {
            generateFileAfterConsolidation();
        }

    }

    @Schedule(
            hour = "*",
            minute = "*",
            second = "16",
            persistent = false)
    @TransactionAttribute(REQUIRES_NEW)
    public void runIndividualQuerys() {
        System.out.println("runIndividualQuerys at " + new Date());
        int singleProcessCount = 200;
        List<IndividualQueryResult> cqrs;
        String j;
        j = "select r "
                + " from IndividualQueryResult r "
                + " where r.included is null "
                + " order by r.id "
                + " ";
        cqrs = getIndividualQueryResultFacade().findByJpql(j, singleProcessCount);
        for (IndividualQueryResult r : cqrs) {
            calculateIndividualQueryResult(r);
        }

    }

    @Schedule(
            hour = "*",
            minute = "*",
            second = "31",
            persistent = false)
    @TransactionAttribute(REQUIRES_NEW)
    public void createIndividualResultsForConsolidatedResults() {
        System.out.println("createIndividualResultsForConsolidatedResults at " + new Date());
        int singleProcessCount = 200;
        List<ConsolidatedQueryResult> cqrs;
        String j;
        j = "select r "
                + " from ConsolidatedQueryResult r "
                + " where r.longValue is null "
                + " order by r.id "
                + " ";
        cqrs = getConsolidatedQueryResultFacade().findByJpql(j, singleProcessCount);

        for (ConsolidatedQueryResult r : cqrs) {
            Long lastIndividualQueryResultId = 0l;
            List<Long> encIds = findEncounterIds(r.getResultFrom(), r.getResultTo(), r.getInstitution());
            for (Long encId : encIds) {
                Long generatedId = createIndividualQueryResultsForConsolidateQueryResult(encId, r.getQueryComponentCode());
                if (generatedId > lastIndividualQueryResultId) {
                    lastIndividualQueryResultId = generatedId;
                }
            }
            r.setLastIndividualQueryResultId(lastIndividualQueryResultId);
            getConsolidatedQueryResultFacade().edit(r);
        }

    }

    @Schedule(
            hour = "*",
            minute = "*",
            second = "46",
            persistent = false)
    @TransactionAttribute(REQUIRES_NEW)
    public void createConsolidatedResultsFromIndividualResults() {
        System.out.println("createConsolidatedResultsFromIndividualResults at " + new Date());
        int singleProcessCount = 200;
        List<ConsolidatedQueryResult> cqrs;
        String j;
        j = "select r "
                + " from ConsolidatedQueryResult r "
                + " where r.longValue is null "
                + " order by r.id "
                + " ";
        cqrs = getConsolidatedQueryResultFacade().findByJpql(j, singleProcessCount);
        Map m;
        for (ConsolidatedQueryResult r : cqrs) {
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
            if (lastIndividualResult.getIncluded() != null) {
                List<Long> encIds = findEncounterIds(r.getResultFrom(), r.getResultTo(), r.getInstitution());
                consolideIndividualResults(r, encIds);
            }
        }
    }

    public void consolideIndividualResults(ConsolidatedQueryResult cr, List<Long> encIds) {
        System.out.println("consolideIndividualResults");
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
//            System.out.println("j = " + j);
//            System.out.println("m = " + m);
            IndividualQueryResult r = getIndividualQueryResultFacade().findFirstByJpql(j, m);
            System.out.println("r = " + r);
            if (r != null) {
                if (r.getIncluded()) {
                    count++;
                }
            }
        }
        System.out.println("count = " + count);
        cr.setLongValue(count);
        getConsolidatedQueryResultFacade().edit(cr);
    }

    private void submitToConsilidate() {
        System.out.println("submit To Consilidate");
        int processingCount = 2;
        processingReport = true;
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
            processingReport = false;
            return;
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
        processingReport = false;

    }

    private void checkCompletenessAfterConsolidation() {
        System.out.println("checkCompletenessAfterConsolidation");
        int processingCount = 2;
        processingReport = true;
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
            processingReport = false;
            return;
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
        processingReport = false;

    }

    private void generateFileAfterConsolidation() {
        System.out.println("generateFileAfterConsolidation");
        int processCount = 2;
        processingReport = true;
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
            processingReport = false;
            return;
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
        processingReport = false;

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

    public void createConsolidatedQueryResult(StoredQueryResult sqr, QueryComponent qc) {
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
            r.setInstitution(sqr.getInstitution());
            r.setArea(sqr.getArea());
            getConsolidatedQueryResultFacade().create(r);
        }

    }

    public boolean checkConsolidatedQueryResult(StoredQueryResult sqr, QueryComponent qc) {
        System.out.println("checkConsolidatedQueryResult");
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
        System.out.println("m = " + m);
        System.out.println("j = " + j);
        ConsolidatedQueryResult r = getConsolidatedQueryResultFacade().findFirstByJpql(j, m);
        System.out.println("r = " + r);
        if (r == null) {
            return false;
        }
        boolean resultFound = r.getLongValue() != null;
        System.out.println("resultFound = " + resultFound);
        return resultFound;
    }

    public Long findConsolidatedQueryResult(StoredQueryResult sqr, QueryComponent qc) {
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

    public Long createIndividualQueryResultsForConsolidateQueryResult(Long encounterId, String qryCode) {

        if (qryCode == null) {
            System.out.println("No qryCode");
            return 0l;
        }
        if (encounterId == null) {
            System.out.println("No encounter id");
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
            getIndividualQueryResultFacade().create(r);
        }

        return r.getId();
    }

    public void calculateIndividualQueryResult(IndividualQueryResult r) {
        List<ClientEncounterComponentBasicDataToQuery> encomps
                = findClientEncounterComponentItems(r.getEncounterId());
        List<QueryComponent> criteria = findCriteriaForQueryComponent(r.getQueryComponentCode());
        if (criteria == null || criteria.isEmpty()) {
            r.setIncluded(true);
        } else {
            r.setIncluded(findMatch(encomps, criteria));
        }
        getIndividualQueryResultFacade().edit(r);
    }

    private Boolean findMatch(List<ClientEncounterComponentBasicDataToQuery> is, List<QueryComponent> qrys) {
        boolean suitableForInclusion = true;
        for (QueryComponent q : qrys) {

            if (q.getItem() == null || q.getItem().getCode() == null) {
                continue;
            } else {
//                    System.out.println("QUERY Item Code is NULL");
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
                System.out.println("Client component Item NOT found for " + q.getItem().getCode());
            }
            if (!thisMatchOk) {
                suitableForInclusion = false;
            }
        }
        return suitableForInclusion;
    }

    private boolean checkRecordCompletenessAfterConsolidation(StoredQueryResult sqr) {
        System.out.println("checkRecordCompletenessAfterConsolidation");
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

            excelFile.close();
            FileOutputStream out = new FileOutputStream(FILE_NAME);
            workbook.write(out);
            out.close();

////                System.out.println("FILE_NAME = " + FILE_NAME);
            InputStream stream;
            stream = new FileInputStream(FILE_NAME);

            Upload u = new Upload();
            u.setFileName(FILE_NAME);
            u.setFileType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
            u.setCreatedAt(new Date());

            getUploadFacade().create(u);

            byte[] byteArray = IOUtils.toByteArray(stream);
            u.setBaImage(byteArray);

//                System.out.println("5 = " + 5);
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

    private Long findReplaceblesInCalculationString(String text, List<EncounterWithComponents> ens) {
        String str;
        Long l = 0l;

        if (ens == null) {
            return l;
        }
        if (ens.isEmpty()) {
            l = 0l;
            return l;
        }

        String patternStart = "#{";
        String patternEnd = "}";
        String regexString = Pattern.quote(patternStart) + "(.*?)" + Pattern.quote(patternEnd);

        Pattern p = Pattern.compile(regexString);

        Matcher m = p.matcher(text);

        while (m.find()) {
            String block = m.group(1);
            str = block;
            QueryComponent qc = findQueryComponentByCode(block);
            if (qc == null) {
                str += " not qc";
                l = null;
//                System.out.println("l = " + l);
                return l;

            } else {
//                System.out.println("qc = " + qc.getName());
                if (qc.getQueryType() == QueryType.Encounter_Count) {
                    List<QueryComponent> criteria = findCriteriaForQueryComponent(qc);

                    if (criteria == null || criteria.isEmpty()) {
                        l = Long.valueOf(ens.size());
                        str += " " + l;
                        return l;
                    } else {
                        l = findMatchingCount(ens, criteria);
                        str += criteria + " " + l;
                        return l;
                    }

                } else {
//                    str += " not encounter count";
                    l = null;
                    return l;
                }
            }

        }

        return l;

    }

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

    private Long findMatchingCount(List<EncounterWithComponents> encs, List<QueryComponent> qrys) {
        Long c = 0l;
        Long encounterCount = 1l;
        for (EncounterWithComponents e : encs) {
            List<ClientEncounterComponentBasicDataToQuery> is = e.getComponents();
            boolean suitableForInclusion = true;
            for (QueryComponent q : qrys) {

                if (q.getItem() == null || q.getItem().getCode() == null) {
                    continue;
                } else {
//                    System.out.println("QUERY Item Code is NULL");
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
//                            System.out.println("i.getItemCode() = " + i.getItemCode());
//                            System.out.println("q.getItem().getCode() = " + q.getItem().getCode());
//                            System.out.println("thisMatchOk = " + thisMatchOk);
                        }
                    }
                }
                if (!componentFound) {
//                    System.out.println("Client component Item NOT found for " + q.getItem().getCode());
                }
                if (!thisMatchOk) {
                    suitableForInclusion = false;
                }
            }
            if (suitableForInclusion) {
                c++;
            }
            encounterCount++;
        }
        return c;
    }

    public boolean matchQuery(QueryComponent q, ClientEncounterComponentBasicDataToQuery clientValue) {
//        System.out.println("Match Query");
//        System.out.println("clientValue = " + clientValue.getItemCode());
        boolean m = false;
        Integer qInt1 = null;
        Integer qInt2 = null;
        Double real1 = null;
        Double real2 = null;
        Long lng1 = null;
        Long lng2 = null;
        Item itemVariable = null;
        Item itemValue = null;

        if (q.getMatchType() == QueryCriteriaMatchType.Variable_Value_Check) {
            switch (q.getQueryDataType()) {
                case integer:
//                    System.out.println("clientValue.getIntegerNumberValue() = " + clientValue.getIntegerNumberValue());

                    qInt1 = q.getIntegerNumberValue();
                    qInt2 = q.getIntegerNumberValue2();
//                    System.out.println("Query int1 = " + qInt1);
//                    System.out.println("Query int2 = " + qInt2);
                    break;
                case item:
//                    System.out.println("clientValue.getItemCode() = " + clientValue.getItemCode());
//                    System.out.println("clientValue.getItemValueCode() = " + clientValue.getItemValueCode());

                    itemValue = q.getItemValue();
                    itemVariable = q.getItem();
                    break;
                case real:
//                    System.out.println("clientValue.getRealNumberValue() = " + clientValue.getRealNumberValue());

                    real1 = q.getRealNumberValue();
                    real2 = q.getRealNumberValue2();
                    break;
                case longNumber:
                    lng1 = q.getLongNumberValue();
                    lng2 = q.getLongNumberValue2();
                    break;

            }
            switch (q.getEvaluationType()) {
                case Equal:
//                    System.out.println("Equal");

                    if (qInt1 != null) {
                        m = qInt1.equals(clientValue.getIntegerNumberValue());
                    }
                    if (lng1 != null) {
                        m = lng1.equals(clientValue.getLongNumberValue());
                    }
                    if (real1 != null) {
                        m = real1.equals(clientValue.getRealNumberValue());
                    }

                    if (itemValue != null && itemVariable != null) {
                        if (clientValue != null
                                && itemValue.getCode() != null
                                && clientValue.getItemValueCode() != null) {

                            if (itemValue.getCode().equals(clientValue.getItemValueCode())) {
                                m = true;
                            }
                        }
                    }
                    break;
                case Less_than:
//                    System.out.println("Less than");
//                    System.out.println("Client Value = " + clientValue.getIntegerNumberValue());
                    if (qInt1 != null && clientValue.getIntegerNumberValue() != null) {
                        m = clientValue.getIntegerNumberValue() < qInt1;
                    }
                    if (lng1 != null && clientValue.getLongNumberValue() != null) {
                        m = clientValue.getLongNumberValue() < lng1;
                    }
                    if (real1 != null && clientValue.getRealNumberValue() != null) {
                        m = clientValue.getRealNumberValue() < real1;
                    }
//                    System.out.println("Included = " + m);
                    break;
                case Between:
//                    System.out.println("Between");
//                    System.out.println("Client Value = " + clientValue.getIntegerNumberValue());
                    if (qInt1 != null && qInt2 != null && clientValue.getIntegerNumberValue() != null) {
                        if (qInt1 > qInt2) {
                            Integer intTem = qInt1;
                            qInt1 = qInt2;
                            qInt2 = intTem;
                        }
                        if (clientValue.getIntegerNumberValue() > qInt1 && clientValue.getIntegerNumberValue() < qInt2) {
                            m = true;
                        }
                    }
                    if (lng1 != null && lng2 != null && clientValue.getLongNumberValue() != null) {
                        if (lng1 > lng2) {
                            Long intTem = lng1;
                            intTem = lng1;
                            lng1 = lng2;
                            lng2 = intTem;
                        }
                        if (clientValue.getLongNumberValue() > lng1 && clientValue.getLongNumberValue() < lng2) {
                            m = true;
                        }
                    }
                    if (real1 != null && real2 != null && clientValue.getRealNumberValue() != null) {
                        if (real1 > real2) {
                            Double realTem = real1;
                            realTem = real1;
                            real1 = real2;
                            real2 = realTem;
                        }
                        if (clientValue.getRealNumberValue() > real1 && clientValue.getRealNumberValue() < real2) {
                            m = true;
                        }
                    }
                    break;
                case Grater_than:
//                    System.out.println("Grater than");
//                    System.out.println("Client Value = " + clientValue.getIntegerNumberValue());
                    if (qInt1 != null && clientValue.getIntegerNumberValue() != null) {
                        m = clientValue.getIntegerNumberValue() > qInt1;
                    }
                    if (real1 != null && clientValue.getRealNumberValue() != null) {
                        m = clientValue.getRealNumberValue() > real1;
                    }
                    break;
                case Grater_than_or_equal:
                    if (qInt1 != null && clientValue.getIntegerNumberValue() != null) {
                        m = clientValue.getIntegerNumberValue() < qInt1;
                    }
                    if (real1 != null && clientValue.getRealNumberValue() != null) {
                        m = clientValue.getRealNumberValue() < real1;
                    }
                case Less_than_or_equal:
                    if (qInt1 != null && clientValue.getIntegerNumberValue() != null) {
                        m = clientValue.getIntegerNumberValue() >= qInt1;
                    }
                    if (real1 != null && clientValue.getRealNumberValue() != null) {
                        m = clientValue.getRealNumberValue() >= real1;
                    }
                    break;
            }
        }
//        System.out.println("Included= " + m);
        return m;
    }

    public List<ClientEncounterComponentBasicDataToQuery> findClientEncounterComponentItemsAlt(Long endId) {
        String j;
        j = "select new lk.gov.health.phsp.pojcs.ClientEncounterComponentBasicDataToQuery("
                + "f.name, "
                + "f.code, "
                + "f.item.code, "
                + "f.shortTextValue, "
                + "f.integerNumberValue, "
                + "f.longNumberValue, "
                + "f.realNumberValue, "
                + "f.booleanValue, "
                + "f.dateValue, "
                + "f.itemValue.code"
                + ") ";

        j += " from ClientEncounterComponentItem f "
                + " where f.retired=false "
                + " and f.encounter.id=:eid";
        Map m = new HashMap();
        m.put("eid", endId);

        List<Object> objs = getClientEncounterComponentItemFacade().findObjects(j, m);
        List<ClientEncounterComponentBasicDataToQuery> t = new ArrayList<>();
        for (Object o : objs) {
            if (o instanceof ClientEncounterComponentBasicDataToQuery) {
                ClientEncounterComponentBasicDataToQuery cbd = (ClientEncounterComponentBasicDataToQuery) o;
                t.add(cbd);
            }
        }
        return t;
    }

    public List<ClientEncounterComponentBasicDataToQuery> findClientEncounterComponentItems(Long endId) {
        String j;

        j = "select f from ClientEncounterComponentItem f "
                + " where f.retired=false "
                + " and f.encounter.id=:eid";
        Map m = new HashMap();
        m.put("eid", endId);
//        System.out.println("m = " + m);
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
//        System.out.println("objs = " + objs.size());
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

//            System.out.println("Name = " + cbd.getName());
//            System.out.println("Code = " + cbd.getCode());
//            System.out.println("cbd = " + cbd.getItemCode());
            ts.add(cbd);
//            System.out.println("ts = " + ts.size());

        }
        return ts;
    }

    public List<QueryComponent> findCriteriaForQueryComponent(QueryComponent p) {
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

    public List<QueryComponent> findCriteriaForQueryComponent(String qryCode) {
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

    public StoredQueryResultFacade getStoreQueryResultFacade() {
        return storeQueryResultFacade;
    }

    public boolean isProcessingReport() {
        return processingReport;
    }

    public void setProcessingReport(boolean processingReport) {
        this.processingReport = processingReport;
    }

    public UploadFacade getUploadFacade() {
        return uploadFacade;
    }

    public EncounterFacade getEncounterFacade() {
        return encounterFacade;
    }

    public QueryComponentFacade getQueryComponentFacade() {
        return queryComponentFacade;
    }

    public ClientEncounterComponentItemFacade getClientEncounterComponentItemFacade() {
        return clientEncounterComponentItemFacade;
    }

    private List<QueryComponent> getQueryComponents() {
        if (queryComponents == null) {
            queryComponents = findAllQueryComponents();
        }

        return queryComponents;
    }

    public void setQueryComponents(List<QueryComponent> queryComponents) {
        this.queryComponents = queryComponents;
    }

    public ConsolidatedQueryResultFacade getConsolidatedQueryResultFacade() {
        return consolidatedQueryResultFacade;
    }

    public IndividualQueryResultFacade getIndividualQueryResultFacade() {
        return individualQueryResultFacade;
    }

}
