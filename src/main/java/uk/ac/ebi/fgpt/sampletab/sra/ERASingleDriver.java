package uk.ac.ebi.fgpt.sampletab.sra;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import javax.sql.DataSource;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.AbstractDriver;
import uk.ac.ebi.fgpt.sampletab.Accessioner;

public class ERASingleDriver  extends AbstractDriver{


    @Argument(required=true, index=0, metaVar="OUTPUT", usage = "output directory")
    protected File outputDir;
    
    @Argument(required=true, index=1, metaVar="SUBMISSION", usage = "submission ID(s)")
    protected List<String> submissionIds;
    
    @Option(name = "--no-conan", usage = "do not trigger conan loads")
    protected boolean noconan = false;

	@Option(name = "--force", aliases = { "-f" }, usage = "force updates")
	protected boolean force = false;
    
    private Logger log = LoggerFactory.getLogger(getClass());

    protected Accessioner accession;
    protected ERADAO eraDom = new ERADAO();

	public ERASingleDriver() {
		
	}
    

	public static void main(String[] args) {
		new ERASingleDriver().doMain(args);
	}
	
    @Override
    public void doMain(String[] args){
        super.doMain(args);
        
        //load defaults for accessioning
        Properties oracleProperties = new Properties();
        try {
            InputStream is = getClass().getResourceAsStream("/sampletabconverters.properties");
            oracleProperties.load(is);
        } catch (IOException e) {
            log.error("Unable to read resource sampletabconverters.properties", e);
            return;
        }
        String hostnameAcc = oracleProperties.getProperty("biosamples.accession.hostname");
        int portAcc = new Integer(oracleProperties.getProperty("biosamples.accession.port"));
        String databaseAcc = oracleProperties.getProperty("biosamples.accession.database");
        String dbusernameAcc = oracleProperties.getProperty("biosamples.accession.username");
        String dbpasswordAcc = oracleProperties.getProperty("biosamples.accession.password");
        
        DataSource ds;
        try {
			ds = Accessioner.getDataSource(hostnameAcc, portAcc, databaseAcc, dbusernameAcc, dbpasswordAcc);
		} catch (ClassNotFoundException e) {
			log.error("Unable to create data source", e);
			return;
		}
        
        accession = new Accessioner(ds);
		
        try {
			eraDom.setup();
		} catch (ClassNotFoundException e) {
			log.error("Unable to create ERA DOM", e);
			return;
		}
        
        
        //actually process it
        for (String submissionId : submissionIds) {
	        if (!submissionId.startsWith("ERA")) {
	        	log.warn("Submission ID must be an ENA submission starting with ERA ("+submissionId+")");
	        	continue;
	        }
			Callable<Void> call = new ERAUpdateCallable(outputDir, submissionId, !noconan, accession, force, eraDom);
			try {
				call.call();
			} catch (Exception e) {
				log.error("Problem processing "+submissionId, e);
			}
			//TODO multithread enable this?
        }
    }

}
