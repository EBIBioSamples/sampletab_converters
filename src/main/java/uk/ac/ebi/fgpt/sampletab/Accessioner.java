package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.Collection;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.GroupNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.arrayexpress2.sampletab.validator.SampleTabValidator;

import com.jolbox.bonecp.BoneCPDataSource;

public class Accessioner {

    private String hostname;
    private int port;
    private String database;
    private String username;
    private String password;

    private final SampleTabValidator validator = new SampleTabValidator();
    
    private final SampleTabSaferParser parser = new SampleTabSaferParser(validator);
    
    private BoneCPDataSource ds = null;
    
    private Connection con = null;
    
    private PreparedStatement stmGetAss = null;
    private PreparedStatement stmGetRef = null;
    private PreparedStatement stmGetGrp = null;
    private PreparedStatement insertAss = null;
    private PreparedStatement insertRef = null;
    private PreparedStatement insertGrp = null;

    private Logger log = LoggerFactory.getLogger(getClass());

    public Accessioner(String host, int port, String database, String username, String password) {
        // Setup the connection with the DB
        this.username = username;
        this.password = password;
        this.hostname = host;
        this.port = port;
        this.database = database;
    }
    
    public void close() {
        if (stmGetAss != null) {
            try {
                stmGetAss.close();
            } catch (SQLException e) {
                //do nothing
            }
            stmGetAss = null;
        }
        if (stmGetRef != null) {
            try {
                stmGetRef.close();
            } catch (SQLException e) {
                //do nothing
            }
            stmGetRef = null;
        }
        if (stmGetGrp != null) {
            try {
                stmGetGrp.close();
            } catch (SQLException e) {
                //do nothing
            }
            stmGetGrp = null;
        }
        if (insertAss != null) {
            try {
                insertAss.close();
            } catch (SQLException e) {
                //do nothing
            }
            insertAss = null;
        }
        if (insertRef != null) {
            try {
                insertRef.close();
            } catch (SQLException e) {
                //do nothing
            }
            insertRef = null;
        }
        if (insertGrp != null) {
            try {
                insertGrp.close();
            } catch (SQLException e) {
                //do nothing
            }
            insertGrp = null;
        }
        
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                //do nothing
            }
            con = null;
        }
        
        if (ds != null) {
            ds.close();
            ds = null;
        }
    }
    
    public void setup() throws SQLException, ClassNotFoundException {

        if (ds == null) {

            Class.forName("oracle.jdbc.driver.OracleDriver");

            String connectURI = "jdbc:oracle:thin:@"+hostname+":"+port+":"+database;
            
            ds = new BoneCPDataSource();
            ds.setJdbcUrl(connectURI);
            ds.setUsername(username);
            ds.setPassword(password);
            
            //remember, there is a limit of 500 on the database
            //set each accessioner to a limit of 10, and always run less than 50 cluster jobs
            ds.setPartitionCount(1); 
            ds.setMaxConnectionsPerPartition(10); 
            ds.setAcquireIncrement(2); 
        }
        
        if (con == null) {
            con = ds.getConnection();
        }
        
        if (stmGetAss == null) stmGetAss = con.prepareStatement("SELECT ACCESSION FROM SAMPLE_ASSAY WHERE USER_ACCESSION LIKE ? AND SUBMISSION_ACCESSION LIKE ?");
        if (stmGetRef == null) stmGetRef = con.prepareStatement("SELECT ACCESSION FROM SAMPLE_REFERENCE WHERE USER_ACCESSION LIKE ? AND SUBMISSION_ACCESSION LIKE ?");
        if (stmGetGrp == null) stmGetGrp = con.prepareStatement("SELECT ACCESSION FROM SAMPLE_GROUPS WHERE USER_ACCESSION LIKE ? AND SUBMISSION_ACCESSION LIKE ?");
        
        if (insertAss == null) insertAss = con.prepareStatement("INSERT INTO SAMPLE_ASSAY ( USER_ACCESSION , SUBMISSION_ACCESSION , DATE_ASSIGNED , IS_DELETED ) VALUES ( ? ,  ? , SYSDATE, 0 )");
        if (insertRef == null) insertRef = con.prepareStatement("INSERT INTO SAMPLE_REFERENCE ( USER_ACCESSION , SUBMISSION_ACCESSION , DATE_ASSIGNED , IS_DELETED ) VALUES ( ? ,  ? , SYSDATE, 0 )");
        if (insertGrp == null) insertGrp = con.prepareStatement("INSERT INTO SAMPLE_GROUPS ( USER_ACCESSION , SUBMISSION_ACCESSION , DATE_ASSIGNED , IS_DELETED ) VALUES ( ? ,  ? , SYSDATE, 0 )");
    }

    protected void singleSample(SampleData sd, SampleNode sample) throws SQLException, ClassNotFoundException{
        if (sample.getSampleAccession() == null) {
            String accession;
            if (sd.msi.submissionReferenceLayer) {
                accession = singleReferenceSample(sample.getNodeName(), sd.msi.submissionIdentifier);
            } else {
                accession = singleAssaySample(sample.getNodeName(), sd.msi.submissionIdentifier);
            }
            sample.setSampleAccession(accession);
        }
    }
    
    public synchronized String singleAssaySample(String name, String submissionID) throws SQLException, ClassNotFoundException {
        //do setup here so correct objects can get passed along
        setup();
        return singleAccession(name, submissionID, "SAMEA", stmGetAss, insertAss);
    }
    
    public synchronized String singleReferenceSample(String name, String submissionID) throws SQLException, ClassNotFoundException {
        //do setup here so correct objects can get passed along
        setup();
        return singleAccession(name, submissionID, "SAME", stmGetRef, insertRef);
    }
    
    public synchronized String singleGroup(String name, String submissionID) throws SQLException, ClassNotFoundException {
        //do setup here so correct objects can get passed along
        setup();     
        return singleAccession(name, submissionID, "SAMEG", stmGetGrp, insertGrp);
    }
    
    /**
     * Internal method to bring the different types of accessioning together into one place
     * 
     * @param name
     * @param submissionID
     * @param prefix
     * @param stmGet
     * @param stmPut
     * @return
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    protected synchronized String singleAccession(String name, String submissionID, String prefix, PreparedStatement stmGet, PreparedStatement stmPut) throws SQLException, ClassNotFoundException {
        if (name == null || name.trim().length() == 0) throw new IllegalArgumentException("name must be at least 1 character");
        if (submissionID == null || submissionID.trim().length() == 0) throw new IllegalArgumentException("submissionID must be at least 1 character");
        if (prefix == null ) throw new IllegalArgumentException("prefix must not be null");
        
        name = name.trim();
        submissionID = submissionID.trim();
        
        String accession = null;
        stmGet.setString(1, name);
        stmGet.setString(2, submissionID);
        log.trace(stmGet.toString());
        ResultSet results = stmGet.executeQuery();
        if (results.next()){
            accession = prefix + results.getInt(1);
            results.close();
        } else {
            log.info("Assigning new accession for "+submissionID+" : "+name);

            //insert it if not exists
            stmPut.setString(1, name);
            stmPut.setString(2, submissionID);
            log.trace(stmPut.toString());
            stmPut.executeUpdate();

            //retreive it
            log.trace(stmGet.toString());
            results = stmGet.executeQuery();
            results.next();
            accession = prefix + results.getInt(1);
            results.close();
        }
        
        return accession;
    }
        
    public SampleData convert(String sampleTabFilename) throws IOException, ParseException, SQLException, ClassNotFoundException {
        return convert(new File(sampleTabFilename));
    }

    public SampleData convert(File sampleTabFile) throws IOException, ParseException, SQLException, ClassNotFoundException {
        return convert(parser.parse(sampleTabFile));
    }

    public SampleData convert(URL sampleTabURL) throws IOException, ParseException, SQLException, ClassNotFoundException {
        return convert(parser.parse(sampleTabURL));
    }

    public SampleData convert(InputStream dataIn) throws ParseException, SQLException, ClassNotFoundException {
        return convert(parser.parse(dataIn));
    }

    public void convert(File inputFile, String outputFilename) throws IOException, ParseException, SQLException, ClassNotFoundException {
        convert(inputFile, new File(outputFilename));
    }

    public void convert(File inputFile, File outputFile) throws IOException, ParseException, SQLException, ClassNotFoundException {
        convert(inputFile, new FileWriter(outputFile));
    }

    public void convert(String inputFilename, Writer writer) throws IOException, ParseException, SQLException, ClassNotFoundException {
        convert(new File(inputFilename), writer);
    }

    public void convert(String inputFilename, File outputFile) throws IOException, ParseException, SQLException, ClassNotFoundException {
        convert(inputFilename, new FileWriter(outputFile));
    }

    public void convert(String inputFilename, String outputFilename) throws IOException, ParseException, SQLException, ClassNotFoundException {
        convert(inputFilename, new File(outputFilename));
    }

    public void convert(SampleData sampleIn, Writer writer) throws IOException, ParseException, SQLException, ClassNotFoundException {
        log.trace("recieved magetab, preparing to convert");
        SampleData sampleOut = convert(sampleIn);
        log.trace("sampletab converted, preparing to output");
        SampleTabWriter sampletabwriter = new SampleTabWriter(writer);
        log.trace("created SampleTabWriter");
        sampletabwriter.write(sampleOut);
        sampletabwriter.close();

    }

    public void convert(File sampletabFile, Writer writer) throws IOException, ParseException, SQLException, ClassNotFoundException {
        log.trace("preparing to load SampleData");
        SampleTabSaferParser stparser = new SampleTabSaferParser();
        log.trace("created SampleTabParser<SampleData>, beginning parse");
        SampleData st = stparser.parse(sampletabFile);
        convert(st, writer);
    }
    
    public SampleData convert(SampleData sd) throws ParseException, SQLException, ClassNotFoundException {
       
        //now assign and retrieve accessions for samples that do not have them
        Collection<SampleNode> samples = sd.scd.getNodes(SampleNode.class);
        for (SampleNode sample : samples) {
            if (sample.getSampleAccession() == null) {
                String accession;
                if (sd.msi.submissionReferenceLayer) {
                    accession = singleReferenceSample(sample.getNodeName(), sd.msi.submissionIdentifier);
                } else {
                    accession = singleAssaySample(sample.getNodeName(), sd.msi.submissionIdentifier);
                }
                sample.setSampleAccession(accession);
            }
        }

        //now assign and retrieve accessions for groups that do not have them
        Collection<GroupNode> groups = sd.scd.getNodes(GroupNode.class);
        for (GroupNode group : groups) {
            if (group.getGroupAccession() == null) {
                String accession = singleGroup(group.getNodeName(), sd.msi.submissionIdentifier);
                group.setGroupAccession(accession);
            }
        }
        return sd;
    }
    
}
