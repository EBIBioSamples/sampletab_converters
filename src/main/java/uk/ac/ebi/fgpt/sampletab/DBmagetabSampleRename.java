package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import oracle.jdbc.pool.OracleDataSource;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.datamodel.graph.Node;
import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SCDNode;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.fgpt.sampletab.arrayexpress.MageTabToSampleTab;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;

public class DBmagetabSampleRename {


    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Option(name = "-i", aliases={"--input"}, usage = "input IDF filename or glob")
    private String inputFilename;
    
    private Logger log = LoggerFactory.getLogger(getClass());

    private OracleDataSource ods = null;
    
    public DBmagetabSampleRename(){

        Properties oracleProperties = new Properties();
        try {
            InputStream is = SampleTabBulk.class.getResourceAsStream("/oracle.properties");
            oracleProperties.load(is);
        } catch (IOException e) {
            log.error("Unable to read resource oracle.properties", e);
        }
        String hostname = oracleProperties.getProperty("hostname");
        Integer port = new Integer(oracleProperties.getProperty("port"));
        String database = oracleProperties.getProperty("database");
        String username = oracleProperties.getProperty("username");
        String password = oracleProperties.getProperty("password");

        String connectURI = "jdbc:oracle:thin:@"+hostname+":"+port+":"+database;

        try {
            ods = new OracleDataSource();
            ods.setURL(connectURI);
            ods.setUser(username);
            ods.setPassword(password);
            // caching params
            // see http://docs.oracle.com/cd/E11882_01/java.112/e16548/concache.htm#CDEBCBJC
            ods.setConnectionCachingEnabled(true);
            ods.setConnectionCacheName("STAccessioning");
            Properties cacheProps = new Properties();
            cacheProps.setProperty("MaxLimit", "5");
            cacheProps.setProperty("ConnectionWaitTimeout", "60");
            //cacheProps.setProperty("ValidateConnection", "true");
            ods.setConnectionCacheProperties(cacheProps);
        } catch (SQLException e) {
            ods = null;
            log.error("Unable to setup oracle data source", e);
        }
    }
        
    private Connection getOracleConnection() throws SQLException {
        Connection c = ods.getConnection();
        if (c == null){
            throw new SQLException("Unable to find connection");
        }
        return c;
    }
    
    private void doNode(Node node, String submissionID){
        PreparedStatement statement = null;

        Connection connect = null;
        try {
            connect = getOracleConnection();
            String newName = node.getNodeName();
            String oldName = null;
            if (newName.contains("source ")){
                oldName = newName.replace("source ", "");
            } else if (newName.contains("sample ")){
                oldName = newName.replace("sample ", "");
            } else {
                log.info("Unable to find old name of "+newName);
                return;
            }
            
            try {
                statement = connect
                        .prepareStatement("UPDATE SAMPLE_ASSAY SET USER_ACCESSION = ? WHERE USER_ACCESSION = ? AND SUBMISSION_ACCESSION = ?");
                statement.setString(1, newName);
                statement.setString(2, oldName);
                statement.setString(3, submissionID);
                int changecount = statement.executeUpdate();
                log.info("No. rows changed: "+changecount);
                statement.close();
            } catch (SQLException e) {
                log.error("Problem changing "+oldName+" to "+newName, e);
            } 
            
        } catch (SQLException e) {
            log.error("Unable to connect to database", e);
        } finally {
            if (statement != null){
                try {
                    statement.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
            if (connect != null){
                try {
                    connect.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
        }
        
        for (Node child : node.getChildNodes()){
            doNode(child, submissionID);
        }
    }
    
    private void doFile(File inputFile) throws IOException, ParseException{

        MageTabToSampleTab mttost = new MageTabToSampleTab();
        SampleData st = mttost.convert(inputFile);
        
        for(SCDNode node : st.scd.getRootNodes()){
            doNode(node, st.msi.submissionIdentifier);
        }
        
    }

    public void doMain(String[] args) {
        
        CmdLineParser parser = new CmdLineParser(this);
        try {
            // parse the arguments.
            parser.parseArgument(args);
            // TODO check for extra arguments?
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            help = true;
        }

        if (help) {
            // print the list of available options
            parser.printSingleLineUsage(System.err);
            System.err.println();
            parser.printUsage(System.err);
            System.exit(1);
            return;
        }
        
        if (this.ods == null){
            System.exit(2);
            return;
        }
        

        
        log.debug("Looking for input files");
        List<File> inputFiles = new ArrayList<File>();
        inputFiles = FileUtils.getMatchesGlob(inputFilename);
        log.info("Found " + inputFiles.size() + " input files from "+inputFilename);
        Collections.sort(inputFiles);
        
        if (inputFiles.size() == 0){
            log.error("No input files found");
            System.exit(3);
            return;
        }

        for (File inputFile : inputFiles) {
            log.info("Checking "+inputFile);
            try {
                doFile(inputFile);
            } catch (IOException e) {
                log.error("Problem parsing "+inputFile, e);
            } catch (ParseException e) {
                log.error("Problem parsing "+inputFile, e);
            }
        }
        
        log.info("Finished processing");
        
        
    }

    public static void main(String[] args) {
        new DBmagetabSampleRename().doMain(args);
    }
    
}
