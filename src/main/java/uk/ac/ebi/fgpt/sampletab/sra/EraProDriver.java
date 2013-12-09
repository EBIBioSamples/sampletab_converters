package uk.ac.ebi.fgpt.sampletab.sra;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.AbstractDriver;

public class EraProDriver extends ENASRACron {

    @Argument(required = true, index = 1, metaVar = "STARTDATE", usage = "Start date as YYYY/MM/DD")
    protected String minDateString;

    @Argument(required = false, index = 2, metaVar = "ENDDATE", usage = "End date as YYYY/MM/DD")
    protected String maxDateString;

    private Logger log = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) {
        new EraProDriver().doMain(args);
    }
    
    public void doGroups(ENASRAGrouper grouper) {
        log.info("Getting sample ids from ERA-PRO");
        EraProManager era = new EraProManager();
        
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
        Date minDate = null;
        Date maxDate = null;
        try {
            minDate = formatter.parse(minDateString);
            if (maxDateString != null) {
                maxDate = formatter.parse(maxDateString);
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        Collection<String> sampleIds = era.getUpdatesSampleId(minDate, maxDate);
        
        grouper.groupSampleIds(sampleIds);
        log.info("Finishing getting sample ids from ERA-PRO");
    }
}
