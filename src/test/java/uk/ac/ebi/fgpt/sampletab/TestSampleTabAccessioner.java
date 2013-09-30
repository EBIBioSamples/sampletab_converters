package uk.ac.ebi.fgpt.sampletab;

import junit.framework.TestCase;


public class TestSampleTabAccessioner extends TestCase {
    public void testTest(){
        
    }
    
	/*
	private URL resource;
    private SampleTabParser parser;

    private String host;
    private int port;
    private String database;
    private String username;
    private String password;
    
    private Logger log = LoggerFactory.getLogger(getClass());

    public void setUp() {
        resource = getClass().getClassLoader().getResource("GEN-ERP001075/sampletab.pre.txt");
        parser = new SampleTabParser();
        Properties mysqlProperties = new Properties();
        try {
            InputStream is = getClass().getResourceAsStream("/oracle.properties");
            mysqlProperties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
            return;
        }
        
        host = mysqlProperties.getProperty("hostname");
        port = new Integer(mysqlProperties.getProperty("port"));
        database = mysqlProperties.getProperty("database");
        username = mysqlProperties.getProperty("username");
        password = mysqlProperties.getProperty("password");
    }

    public void tearDown() {
        resource = null;
        parser = null;
    }

    public void testAccessioning() {
    	try {
            Accessioner accessioner;
            try {
                accessioner = new Accessioner(host, port, database, username, password);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                fail();
                return;
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
                return;
            }
            
    	    SampleData st = parser.parse(resource);
    	    accessioner.convert(st);
    	    
			StringWriter out = new StringWriter();
			SampleTabWriter sampletabwriter = new SampleTabWriter(out);
			try {
				sampletabwriter.write(st);
			} catch (IOException e) {
                log.error("Unable to write output", e);
				fail();
			}
			//System.out.println(out.toString());
		} catch (ParseException e) {
		    log.error("Unable to parse source", e);
            fail();
		} catch (SQLException e) {
		    log.error(e.getSQLState(), e);
            fail();
        } 
    }
*/
}
