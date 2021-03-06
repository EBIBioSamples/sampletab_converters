package uk.ac.ebi.fgpt.sampletab;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.MaterialAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SexAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.UnitAttribute;

import com.jolbox.bonecp.BoneCPDataSource;

public class CorrectorAddAttr {

    private String hostname;
    private int port;
    private String database;
    private String username;
    private String password;

    private BoneCPDataSource ds = null;

    private Logger log = LoggerFactory.getLogger(getClass());

    public CorrectorAddAttr(String host, int port, String database, String username, String password) {
        // Setup the connection with the DB
        this.username = username;
        this.password = password;
        this.hostname = host;
        this.port = port;
        this.database = database;
    }
    
    protected void doSetup() {
        synchronized(this) {
            if (ds == null) {
                try {
                    Class.forName("oracle.jdbc.driver.OracleDriver");
                } catch (ClassNotFoundException e) {
                    log.error("Unable to find oracle.jdbc.driver.OracleDriver", e);
                    return;
                }

                String connectURI = "jdbc:oracle:thin:@"+hostname+":"+port+":"+database;
                
                ds = new BoneCPDataSource();
                ds.setJdbcUrl(connectURI);
                ds.setUsername(username);
                ds.setPassword(password);
                
                //remember, there is a limit of 500 on the database
                //set each accessioner to a limit of 10, and always run less than 50 cluster jobs
                ds.setPartitionCount(1); 
                ds.setMaxConnectionsPerPartition(10); 
                ds.setAcquireIncrement(1); 
            }
        }
    }

    public void addAttribute(SampleData st) {
        doSetup();
        
        String acc = st.msi.submissionIdentifier;
        String sql = "SELECT * FROM ATTR_ADD WHERE SAMPLE_ID = '"+acc+"'";
        Connection conn = null;
        Statement statement = null;
        ResultSet results = null;
        try {
            conn = ds.getConnection();
            statement = conn.createStatement();
            results = statement.executeQuery(sql);
            
            while (results.next()) {
                String key = results.getString("ATTR_KEY");
                String value = results.getString("ATTR_VALUE");
                                
                log.info("Adding "+key+" : "+value+" to "+acc);

                if (key.toLowerCase().equals("submission description")) {
                    st.msi.submissionDescription = value;
                } else if (key.toLowerCase().equals("submission title")) {
                    st.msi.submissionTitle = value;
                } else {
                    log.warn("Unable to add "+key+" : "+value+" to "+acc);
                }
            }
        } catch (SQLException e) {
            log.error("Problem running sql", e);
        } finally {
            if (results != null) {
                try {
                    results.close();
                } catch (SQLException e) {
                    // do nothing
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    // do nothing
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // do nothing
                }
            }
        }
        
    }
    
    
    public void addAttribute(SampleData st, SampleNode sample) {
        
        log.trace("Checking for attributes to add to "+sample.getSampleAccession());
        String acc = sample.getSampleAccession();
        if (acc == null || acc.trim().length() == 0) {
            return;
        }
        log.trace("Checking for attributes to add to "+acc);
        
        //only add attributes if there is at least one already
        //this avoid confusion with refernece vs owner user of samples
        
        if (sample.getAttributes().size() == 0) {
        	log.info("Sample "+acc+" is a reference, skipping");
        	return;
        }
        
        //only do setup once we are ready to add attributes
        doSetup();
        
        String sql = "SELECT * FROM ATTR_ADD WHERE SAMPLE_ID = '"+acc+"'";
        Connection conn = null;
        Statement statement = null;
        ResultSet results = null;
        try {
            conn = ds.getConnection();
            statement = conn.createStatement();
            results = statement.executeQuery(sql);
            
            while (results.next()) {
                String key = results.getString("ATTR_KEY");
                String value = results.getString("ATTR_VALUE");
                String termsourceREF = results.getString("TERM_SOURCE_REF");
                String termsourceID = results.getString("TERM_SOURCE_ID");
                String termsourceURI = results.getString("TERM_SOURCE_URI");
                String termsourceVersion = results.getString("TERM_SOURCE_VERSION");
                String unit = results.getString("UNIT");
                
                TermSource termSource = null;
                
                if (termsourceREF != null && termsourceID != null && termsourceURI != null) {
                    termSource = new TermSource(termsourceREF, termsourceURI, termsourceVersion);
                }
                
                log.info("Adding "+key+" : "+value+" to "+acc);

                if (key.toLowerCase().equals("sex")) {
                    SexAttribute attr = new SexAttribute(value);
                    if (termSource != null) {
                        attr.setTermSourceREF(st.msi.getOrAddTermSource(termSource));
                        attr.setTermSourceID(termsourceID);
                    }
                    sample.addAttribute(attr);
                } else if (key.toLowerCase().equals("material")) {
                    MaterialAttribute attr = new MaterialAttribute(value);
                    if (termSource != null) {
                        attr.setTermSourceREF(st.msi.getOrAddTermSource(termSource));
                        attr.setTermSourceID(termsourceID);
                    }
                    sample.addAttribute(attr);
                } else if (key.toLowerCase().equals("sample name")) {
                    //automatically make the existing name a synonym
                    sample.addAttribute(new CommentAttribute("synonym", sample.getNodeName()), 0);
                    sample.setNodeName(value);
                } else if (key.toLowerCase().startsWith("characteristic[")){
                    String keyTrim = key.substring(15, key.length()-1);
                    CharacteristicAttribute attr = new CharacteristicAttribute(keyTrim, value);
                    if (unit != null) {
                        attr.unit = new UnitAttribute(unit);
                        if (termSource != null) {
                            attr.unit.setTermSourceREF(st.msi.getOrAddTermSource(termSource));
                            attr.unit.setTermSourceID(termsourceID);
                        }
                    } else if (termSource != null) {
                        attr.setTermSourceREF(st.msi.getOrAddTermSource(termSource));
                        attr.setTermSourceID(termsourceID);
                    }
                    sample.addAttribute(attr);
                } else if (key.toLowerCase().startsWith("comment[")){
                    String keyTrim = key.substring(8, key.length()-1);
                    CommentAttribute attr = new CommentAttribute(keyTrim, value);
                    if (unit != null) {
                        attr.unit = new UnitAttribute(unit);
                        if (termSource != null) {
                            attr.unit.setTermSourceREF(st.msi.getOrAddTermSource(termSource));
                            attr.unit.setTermSourceID(termsourceID);
                        }
                    } else if (termSource != null) {
                        attr.setTermSourceREF(st.msi.getOrAddTermSource(termSource));
                        attr.setTermSourceID(termsourceID);
                    }
                    sample.addAttribute(attr);
                } else {
                    log.warn("Unable to add "+key+" : "+value+" to "+acc);
                }
            }
        } catch (SQLException e) {
            log.error("Problem running sql", e);
        } finally {
            if (results != null) {
                try {
                    results.close();
                } catch (SQLException e) {
                    // do nothing
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    // do nothing
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // do nothing
                }
            }
        }
    }
}
