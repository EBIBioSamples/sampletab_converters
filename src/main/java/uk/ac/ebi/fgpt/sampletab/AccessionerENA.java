package uk.ac.ebi.fgpt.sampletab;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

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
        if (submission.matches("^GEN-[ESD]R[AP][0-9]{6,}$")){
            log.trace("Identified an ENA SRA submission : "+submission);
            return true;
        } else if (submission.matches("^GEM-.*$")){
            log.trace("Identified an ENA EMBL-Bank submission : "+submission);
            return true;
        }
        return false;
    }

    @Override
    protected void singleSample(SampleData sd, SampleNode sample, String submissionID, String prefix, String table, int retries, Connection connect, DataSource ds) throws SQLException{
        if (isENA(submissionID)){
            submissionID = "ENA";
        } 
        super.singleSample(sd, sample, submissionID, prefix, table, retries, connect, ds);
    }

    @Override
    protected void bulkSamples(SampleData sd, String submissionID, String prefix, String table, int retries, Connection connect, DataSource ds) throws SQLException {
        if (isENA(submissionID)){
            submissionID = "ENA";
            //don't do as a bulk, but iterate over samples
            //doesn't report missing samples, but is tractable
        } else {
            super.bulkSamples(sd, submissionID, prefix, table, retries, connect, ds);
        }
    }
    
}
