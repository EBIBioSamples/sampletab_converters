package uk.ac.ebi.fgpt.sampletab;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccessionerENA extends Accessioner {

    private Logger log = LoggerFactory.getLogger(getClass());

    public AccessionerENA(String host, int port, String database, String username, String password) {
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
    protected synchronized String singleAccession(String name, String submissionID, String prefix, 
            PreparedStatement stmGet, PreparedStatement stmPut) throws SQLException, ClassNotFoundException {
        if (isENA(submissionID)) {
            submissionID = "ENA";
        }
        return super.singleAccession(name, submissionID, prefix, stmGet, stmPut);
    }
    
}
