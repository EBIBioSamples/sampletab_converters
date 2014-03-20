package uk.ac.ebi.fgpt.sampletab.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCPDataSource;

import uk.ac.ebi.fgpt.sampletab.AbstractDriver;

public class AttributeDBSummary extends AbstractDriver {

    @Argument(required=true, index=0, metaVar="OUTPUT", usage = "output filename")
    private File outputFile;
    
    @Option(name = "-a", aliases={"--attributes"}, usage = "number of attributes")
    private int rows = 100;
    
    @Option(name = "-v", aliases={"--values"}, usage = "number of values of attributes")
    private int cols = 100;
    
    private Map<String, Map<String, Integer>> map = new HashMap<String, Map<String, Integer>>();
    private BoneCPDataSource ds = null;
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    protected synchronized BoneCPDataSource getDataSource()
            throws ClassNotFoundException {
        if (ds == null) {
            synchronized (getClass()) {
                // load defaults
                Properties properties = new Properties();
                try {
                    InputStream is = getClass().getResourceAsStream("/hibernate_bioSD.properties");
                    properties.load(is);
                } catch (IOException e) {
                    log.error("Unable to read resource era-pro.properties", e);
                }
                String username = properties.getProperty("hibernate.connection.username");
                String password = properties.getProperty("hibernate.connection.password");
                String driverClass = properties.getProperty("hibernate.connection.driver_class");
                String jdbc = properties.getProperty("hibernate.connection.url");

                Class.forName(driverClass);
                
                log.trace("JDBC URL = " + jdbc);
                log.trace("USER = " + username);
                log.trace("PW = " + password);

                ds = new BoneCPDataSource();
                ds.setJdbcUrl(jdbc);
                ds.setUsername(username);
                ds.setPassword(password);

                ds.setPartitionCount(1);
                ds.setMaxConnectionsPerPartition(10);
                ds.setAcquireIncrement(2);
            }
        }
        return ds;
    }

    public static void main(String[] args) {
        new AttributeDBSummary().doMain(args);
    }
    
    @Override
    protected void doMain(String[] args) {
        super.doMain(args);
        log.info("Starting checking of attribtue of database");
        //connect to database

        DataSource datasource = null;
        try { 
            datasource = getDataSource();
        } catch (ClassNotFoundException e) {
            log.error("Problem connecting to database", e);
            return;
        }
        
        Connection conn = null;
        Statement statement = null;
        ResultSet rs = null;
        try {
            conn = datasource.getConnection();
            statement = conn.createStatement();
                        
            //get key/value pairs           

            String sql = "SELECT DB_REC_REF.DB_NAME, EXP_PROP_VAL.TERM_TEXT, EXP_PROP_TYPE.TERM_TEXT, BIO_PRODUCT.ACC " +
            	"FROM EXP_PROP_TYPE, EXP_PROP_VAL, PRODUCT_PV, BIO_PRODUCT, SAMPLE_DB_REC_REF, DB_REC_REF " +
            	"WHERE EXP_PROP_VAL.TYPE_ID = EXP_PROP_TYPE.ID " +
            	"AND EXP_PROP_VAL.ID = PRODUCT_PV.PV_ID " +
            	"AND PRODUCT_PV.OWNER_ID = BIO_PRODUCT.ID " +
            	"AND BIO_PRODUCT.ID = SAMPLE_DB_REC_REF.SAMPLE_ID " +
            	"AND SAMPLE_DB_REC_REF.DB_REC_ID = DB_REC_REF.ID ";            
            
            
            rs = statement.executeQuery(sql);
            if (rs == null) {
                throw new RuntimeException("Problem getting attribute type/value pairs");
            }

            log.info("Executed initial SQL query");
            
            //add to hashmap
            while (rs.next()) {
                //results are 1-indexed
                String value= rs.getString(2);
                String key = rs.getString(1)+":"+rs.getString(3); //SOURCE:TYPE
                log.trace("Got "+key+" / "+value);

                if (!map.containsKey(key)) {
                    map.put(key,new HashMap<String, Integer>());
                }
                if (map.get(key).containsKey(value)) {
                        int count = map.get(key).get(value).intValue();
                        map.get(key).put(value,
                                count + 1);
                } else {
                    map.get(key).put(value, new Integer(1));
                }
                
                //if (map.size() % 1000 == 0) {
                //    log.info("map contains "+map.size()+" items");
                //    break;
                //}
            }
            
            
        } catch (SQLException e) {
            log.error("Problem with database", e);
            return;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
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
            if (ds != null) {
                ds.close();
            }
        }
        

        
        //output hashmap
        Writer writer = null;
        
        ArrayList<String> keys = new ArrayList<String>(map.keySet());
        Collections.sort(keys, new AttributeSummary.KeyComparator(map));
        Collections.reverse(keys);
        
        try {
            writer = new BufferedWriter(new FileWriter(outputFile));
            
            int rowcount = 0;
            for (String key : keys){
                int total = 0;
                for (Integer value : map.get(key).values()){
                    total += value;
                }
                writer.write(key+" ("+total+")\t");
                
                int colcount = 0;
                ArrayList<String> values = new ArrayList<String>(map.get(key).keySet());
                Collections.sort(values, new AttributeSummary.ValueComparator(map.get(key)));
                Collections.reverse(values);
                
                for (String value : values){
                    writer.write(value+" ("+map.get(key).get(value)+")\t");
                    colcount += 1;
                    if (this.cols > 0 && colcount > this.cols){
                        break;
                    }
                }
                
                writer.write("\n");
                rowcount += 1;
                if (this.rows > 0 && rowcount > this.rows){
                    break;
                }
            }
            
        } catch (IOException e) {
            log.error("Problem writing to "+outputFile, e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    //do nothing
                }
            }
        }
        
    }
}
