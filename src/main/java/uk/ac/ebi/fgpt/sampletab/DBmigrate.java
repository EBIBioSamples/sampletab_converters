package uk.ac.ebi.fgpt.sampletab;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBmigrate {

    private Logger log = LoggerFactory.getLogger(getClass());
        
    private Connection getProdOracleConnection() throws ClassNotFoundException, SQLException {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            throw e;
        }
        
        log.info("Found oracle.jdbc.driver.OracleDriver");

        String hostname = "burns.ebi.ac.uk";
        String port = "1521";
        String database = "DWEP";
        String username = "bsd_acc";
        String password = "bsd_acc";
        
        String url = "jdbc:oracle:thin:@"+hostname+":"+port+":"+database;

        Connection con = null;
        try {
            con = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            log.error("Unable to connect to "+url, e);
            throw e;
        }

        log.info("connected to "+url+" with username/password "+username+"/"+password);
        return con;
    }
    
    private Connection getTestOracleConnection() throws ClassNotFoundException, SQLException {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            throw e;
        }
        
        log.info("Found oracle.jdbc.driver.OracleDriver");

        String hostname = "bart.ebi.ac.uk";
        String port = "1521";
        String database = "BIOSDTST";
        String username = "bsd_acc";
        String password = "bsd_acc";
        
        String url = "jdbc:oracle:thin:@"+hostname+":"+port+":"+database;

        Connection con = null;
        try {
            con = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            log.error("Unable to connect to "+url, e);
            throw e;
        }

        log.info("connected to "+url+" with username/password "+username+"/"+password);
        return con;
    }
    
    private void copyTable(String table_name, String seq_name, Connection target, Connection source){

        Statement sourcestatement = null;
        Statement targetstatement = null;
        ResultSet rs;
        String sql;
        try {
            sourcestatement = source.createStatement();
            targetstatement = target.createStatement();
            
            //try to drop the table if it exists
            try {
                sql = "DROP TABLE "+table_name+"";
                log.info(sql);
                targetstatement.execute(sql);
                //when droping a table, all indexs are dropped automatically
                //sql = "DROP INDEX PK_"+table_name+"_ACC";
                //log.info(sql);
                //targetstatement.execute(sql);
                //sql = "DROP INDEX UK_"+table_name+"_USACC";
                //log.info(sql);
                //targetstatement.execute(sql);
            } catch (SQLException e){
                if (e.getErrorCode() == 942){
                    //table does not exist error
                    //do nothing
                } else {
                    throw e;
                }
            }

            //make sure any triggers are dropped before adding the data
            try {
                sql = "DROP TRIGGER  TRG_"+table_name+"_ACC";
                log.info(sql);
                targetstatement.execute(sql);
            } catch (SQLException e){
                if (e.getErrorCode() == 4080){
                    //trigger does not exist error
                    //do nothing
                } else {
                    throw e;
                }
            }
            
            //try to drop sequence if it exists
            try {
                sql = "DROP SEQUENCE "+seq_name;
                log.info(sql);
                targetstatement.execute(sql);
            } catch (SQLException e){
                if (e.getErrorCode() == 2289){
                    //sequence does not exist error
                    //do nothing
                } else {
                    throw e;
                }
            }
            
            
            sql = "CREATE TABLE "+table_name+" ( accession NUMBER(11) NOT NULL, user_accession VARCHAR2(255) NOT NULL, submission_accession VARCHAR2(255) NOT NULL, date_assigned date NOT NULL, is_deleted NUMBER(11) DEFAULT 0 NOT NULL) TABLESPACE BSDACC_DATA NOCOMPRESS";
            log.info(sql);
            targetstatement.execute(sql);

            sql = "CREATE UNIQUE INDEX PK_"+table_name+"_ACC ON "+table_name+" (ACCESSION) TABLESPACE BSDACC_INDX";
            log.info(sql);
            targetstatement.execute(sql);

            sql = "CREATE UNIQUE INDEX UK_"+table_name+"_USACC ON "+table_name+" (USER_ACCESSION, SUBMISSION_ACCESSION) TABLESPACE BSDACC_INDX";
            log.info(sql);
            targetstatement.execute(sql);

            sql = "ALTER TABLE "+table_name+" ADD ( CONSTRAINT PK_"+table_name+"_ACC PRIMARY KEY (ACCESSION) USING INDEX PK_"+table_name+"_ACC ENABLE VALIDATE )";
            log.info(sql);
            targetstatement.execute(sql);

            sql = "ALTER TABLE "+table_name+" ADD ( CONSTRAINT UK_"+table_name+"_USACC UNIQUE (USER_ACCESSION, SUBMISSION_ACCESSION ) USING INDEX UK_"+table_name+"_USACC ENABLE VALIDATE )";
            log.info(sql);
            targetstatement.execute(sql);
            
            rs = sourcestatement.executeQuery("SELECT * FROM "+table_name.toLowerCase());
            while (rs.next()) {
                Integer accession = rs.getInt("accession");
                String userAccession = rs.getString("user_accession");
                userAccession = userAccession.replace("'", "''");
                String submissionAccession = rs.getString("submission_accession");
                submissionAccession = submissionAccession.replace("'", "''");
                Date assigned = rs.getDate("date_assigned");
                Integer deleted = rs.getInt("is_deleted");
                
                //TODO maybe make this a prepared statement for speed?
                sql = "INSERT INTO "+table_name+" VALUES ( '"+accession+"' , '"+userAccession+"' , '"+submissionAccession+"' , to_date('"+assigned+"', 'YYYY-MM-DD') , '"+deleted+"' )";
                log.debug(sql);
                targetstatement.execute(sql);
            }
            
            //now update sequence to match
            //rs = sourcestatement.executeQuery("SELECT LAST_NUMBER FROM user_sequences WHERE SEQUENCE_NAME = '"+seq_name+"'");
            rs = sourcestatement.executeQuery("SELECT MAX(ACCESSION) FROM "+table_name.toLowerCase());
            if (rs.next()) {
                Integer lastNumber = rs.getInt(1);
                //not sure if we have to increment, but better safe than sorry.
                lastNumber += 1;
                sql = "CREATE SEQUENCE "+seq_name+" START WITH "+lastNumber+" MAXVALUE 1000000000000000000000000000 MINVALUE 1 NOCYCLE CACHE 20 NOORDER";
                log.info(sql);
                targetstatement.execute(sql);
            }
            
            
            //now create the trigger that uses the sequence
            
            sql = "CREATE TRIGGER TRG_"+table_name+"_ACC BEFORE INSERT ON "+table_name+" REFERENCING OLD AS OLD NEW AS NEW FOR EACH ROW BEGIN SELECT "+seq_name+".nextval INTO :new.ACCESSION FROM dual; END;";
            log.info(sql);
            targetstatement.execute(sql);
            
        } catch (SQLException e) {
            log.error("Unable to process SQL", e);
            return;
        } finally {
            if (sourcestatement != null){
                try {
                    sourcestatement.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
            if (targetstatement != null){
                try {
                    targetstatement.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
        }
    }
    
    public void doMain() {
        Connection source = null;
        Connection target = null;   
        
        try {
            //source = getProdMySQLConnection();
            source = getProdOracleConnection();
        } catch (ClassNotFoundException e) {
            log.error("Unable to find driver", e);
            return;
        } catch (SQLException e) {
            log.error("Unable to connect to execute", e);
            return;
        }  
        
        try {
            target = getTestOracleConnection();
            //target = getProdOracleConnection();
        } catch (ClassNotFoundException e) {
            log.error("Unable to find driver", e);
            return;
        } catch (SQLException e) {
            log.error("Unable to connect to execute", e);
            return;
        }
   

        copyTable("SAMPLE_GROUPS", "SEQ_GROUPS", target, source);
        copyTable("SAMPLE_REFERENCE", "SEQ_REFERENCE", target, source);
        copyTable("SAMPLE_ASSAY", "SEQ_ASSAY", target, source);
    }

    public static void main(String[] args) {
        new DBmigrate().doMain();
    }
    
}
