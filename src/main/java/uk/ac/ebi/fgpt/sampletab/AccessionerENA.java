package uk.ac.ebi.fgpt.sampletab;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;

public class AccessionerENA extends Accessioner {

    private Logger log = LoggerFactory.getLogger(getClass());

    public AccessionerENA(String host, int port, String database, String username, String password)
            throws ClassNotFoundException, SQLException {
        super(host, port, database, username, password);
    }

    //special case for ENA subs
    private boolean isENA(String submission) {
        if (submission.matches("^GEN-[ESD]RP[0-9]{6,}$")){
            log.info("Identified an ENA submission :"+submission);
            return true;
        }
        return false;
    }

    @Override
    protected void singleSample(SampleData sd, SampleNode sample, String submissionID, String prefix, String table, int retries) throws SQLException{
        if (isENA(submissionID)){
            submissionID = "ENA";
        } 
        super.singleSample(sd, sample, submissionID, prefix, table, retries);
    }

    @Override
    protected void bulkSamples(SampleData sd, String submissionID, String prefix, String table, int retries) throws SQLException {
        if (isENA(submissionID)){
            submissionID = "ENA";
            //don't do as a bulk, but iterate over samples
            //doesn't report missing samples, but is tractable
        } else {
            super.bulkSamples(sd, submissionID, prefix, table, retries);
        }
    }
    
}
