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
import lk.gov.health.phsp.entity.Client;
import lk.gov.health.phsp.entity.ClientEncounterComponentItem;
import lk.gov.health.phsp.entity.Encounter;
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
import lk.gov.health.phsp.facade.EncounterFacade;
import lk.gov.health.phsp.facade.QueryComponentFacade;
import lk.gov.health.phsp.facade.StoredQueryResultFacade;
import lk.gov.health.phsp.facade.UploadFacade;
import lk.gov.health.phsp.pojcs.EncounterWithComponents;
import lk.gov.health.phsp.pojcs.ReportTimePeriod;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import lk.gov.health.phsp.pojcs.ClientEncounterComponentBasicDataToQuery;

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

    private List<QueryComponent> queryComponents;

    @Schedule(
            hour = "*",
            minute = "*",
            second = "10",
            persistent = false)
    public void runEveryMinute() {
        System.out.println("Schedule Running at " + new Date());
        queryComponents = null;

        if (!processingReport) {
            runReports();
        }

    }

    public void runReports() {

        processingReport = true;
        String j;
        Map m = new HashMap();
        j = "select q from StoredQueryResult q "
                + " where q.retired=false "
                + " and q.processFailed=false "
                + " and q.processCompleted=false "
                + " and q.processStarted=false "
                + " order by q.id";
        List<StoredQueryResult> qs = getStoreQueryResultFacade().findByJpql(j);
////            System.out.println("qs = " + qs);
        if (qs == null) {
            processingReport = false;
            return;
        }

        for (StoredQueryResult q : qs) {
            System.out.println("Query Starting at = " + new Date());
            System.out.println("Query name = " + q.getQueryComponent().getName());
            System.out.println("Query requested by = " + q.getCreater().getPerson().getName());
            System.out.println("Query requested period = " + q.getPeriodString());
            System.out.println("Query requested institution = " + q.getInstitution().getName());
            System.out.println("Query requested at = " + q.getCreatedAt());
            q.setProcessStarted(true);
            q.setProcessStartedAt(new Date());
            q.setProcessFailed(false);
            q.setProcessCompleted(false);
            getStoreQueryResultFacade().edit(q);
            boolean processSuccess = processReport(q);

            if (processSuccess) {
                q.setProcessCompleted(true);
                q.setProcessCompletedAt(new Date());
                getStoreQueryResultFacade().edit(q);
                System.out.println("Query Completed at = " + new Date());
            } else {
                q.setProcessFailed(true);
                q.setProcessFailedAt(new Date());
                getStoreQueryResultFacade().edit(q);
                System.out.println("Query Failed at = " + new Date());
            }
        }
        processingReport = false;

    }

    private boolean processReport(StoredQueryResult sqr) {
//       System.out.println("sqr = " + sqr);
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
            System.out.println("No Report Available");
            getStoreQueryResultFacade().edit(sqr);
            return success;
        }

        if (queryComponent.getQueryType() == null) {
            sqr.setErrorMessage("No query type specified.");
            System.out.println("No query type specified.");
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
            System.out.println("No excel template uploaded.");
            getStoreQueryResultFacade().edit(sqr);
            return success;
        }

        List<EncounterWithComponents> encs = new ArrayList<>();

        switch (queryComponent.getQueryType()) {
            case Encounter_Count:
                List<Long> tes = findEncounterIds(rtp.getFrom(), rtp.getTo(), ins);
                for (Long e : tes) {
                    EncounterWithComponents enc = new EncounterWithComponents();
                    enc.setComponents(findClientEncounterComponentItems(e));
                    encs.add(enc);
                }

                break;
            case Client_Count:
                sqr.setErrorMessage("Client Queries not yet supported.");
                getStoreQueryResultFacade().edit(sqr);
                return success;
            default:
                sqr.setErrorMessage("This type of query not yet supported.");
                getStoreQueryResultFacade().edit(sqr);
                return success;
        }

        if (encs.size() < 1) {
            sqr.setErrorMessage("No Data.");
            System.out.println("No data. empty");
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
////                            System.out.println("ct = " + ct);
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
                        Long temLong = findReplaceblesInCalculationString(cellString, encs);
                        if (temLong != null) {
                            currentCell.setCellValue(temLong);
                        } else {
////                                System.out.println("temLong is null.");
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
//                System.out.println("6 = " + 6);
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

    public Long findReplaceblesInCalculationString(String text, List<EncounterWithComponents> ens) {
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

    public QueryComponent findQueryComponentByCode(String code) {
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

    private Long findMatchingCount(List<EncounterWithComponents> encs, List<QueryComponent> qrys) {
        Long c = 0l;
        for (EncounterWithComponents e : encs) {
            List<ClientEncounterComponentBasicDataToQuery> is = e.getComponents();
            boolean suitableForInclusion = true;
            for (QueryComponent q : qrys) {

                if (q.getItem() == null || q.getItem().getCode() == null) {
                    System.out.println("Item code NULL");
                    continue;
                } else {
                    System.out.println("QUERY Item Code is NULL");
                }

                boolean thisMatchOk = false;
                boolean componentFound;
                for (ClientEncounterComponentBasicDataToQuery i : is) {
                    componentFound=false;
                    
                    if (i.getItemCode() == null) {
                        continue;
                    }

                    if (i.getItemCode().trim().equalsIgnoreCase(q.getItem().getCode().trim())) {
                        componentFound=true;
                        if (matchQuery(q, i)) {
                            thisMatchOk = true;
                        }
                    } else {
                        System.out.println("No Match");
                    }
                }
                if (!thisMatchOk) {
                    suitableForInclusion = false;
                }
            }
            if (suitableForInclusion) {
                c++;
            }
        }
        return c;
    }

    public boolean matchQuery(QueryComponent q, ClientEncounterComponentBasicDataToQuery clientValue) {
        System.out.println("Match Query");
        System.out.println("q = " + q.getCode());
        System.out.println("clientValue = " + clientValue.getItemCode());
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

    public List<ClientEncounterComponentBasicDataToQuery> findClientEncounterComponentItems(Long endId) {
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

        List<Object> objs = getClientEncounterComponentItemFacade().findAggregates(j, m);
        List<ClientEncounterComponentBasicDataToQuery> t = new ArrayList<>();
        for (Object o : objs) {
            if (o instanceof ClientEncounterComponentBasicDataToQuery) {
                ClientEncounterComponentBasicDataToQuery cbd = (ClientEncounterComponentBasicDataToQuery) o;
                t.add(cbd);
            }
        }
        return t;
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

}
