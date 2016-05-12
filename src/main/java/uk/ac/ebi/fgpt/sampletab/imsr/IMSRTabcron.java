package uk.ac.ebi.fgpt.sampletab.imsr;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Date;
import java.util.Properties;

import javax.sql.DataSource;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.Accessioner;
import uk.ac.ebi.fgpt.sampletab.utils.ConanUtils;

public class IMSRTabcron {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Option(name = "-o", aliases={"--output"}, usage = "output directory")
    private String outputDirName;

    @Option(name = "--no-conan", usage = "do not trigger conan loads?")
    private boolean noconan = false;
    



    @Option(name = "--hostname", aliases={"-n"}, usage = "server hostname")
    private String hostname;

    @Option(name = "--port", usage = "server port")
    private Integer port;

    @Option(name = "--database", aliases={"-d"}, usage = "server database")
    private String database;

    @Option(name = "--username", aliases={"-u"}, usage = "server username")
    private String dbusername;

    @Option(name = "--password", aliases={"-p"}, usage = "server password")
    private String dbpassword;
    
    
    
	private Logger log = LoggerFactory.getLogger(getClass());

	private IMSRTabcron() {
	}
	

	public static void main(String[] args) {
        new IMSRTabcron().doMain(args);
    }
	
	private void submitConan(String submissionIdentifier){
        try {
            ConanUtils.submit(submissionIdentifier, "BioSamples (other)");
        } catch (IOException e) {
            log.error("Problem submitting "+submissionIdentifier, e);
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
            System.err.println();
            System.exit(1);
            return;
        }
        
		File outdir = new File(this.outputDirName);

		if (outdir.exists() && !outdir.isDirectory()) {
			System.err.println("Target is not a directory");
			System.exit(1);
			return;
		}

		if (!outdir.exists())
			outdir.mkdirs();
		

        //load defaults
        Properties oracleProperties = new Properties();
        try {
            InputStream is = getClass().getResourceAsStream("/sampletabconverters.properties");
            oracleProperties.load(is);
        } catch (IOException e) {
            log.error("Unable to read resource sampletabconverters.properties", e);
        }
        if (hostname == null){
            hostname = oracleProperties.getProperty("biosamples.accession.hostname");
        }
        if (port == null){
            port = new Integer(oracleProperties.getProperty("biosamples.accession.port"));
        }
        if (database == null){
            database = oracleProperties.getProperty("biosamples.accession.database");
        }
        if (dbusername == null){
            dbusername = oracleProperties.getProperty("biosamples.accession.username");
        }
        if (dbpassword == null){
            dbpassword = oracleProperties.getProperty("biosamples.accession.password");
        }
        
        DataSource ds = null;
		try {
			ds = Accessioner.getDataSource(hostname, 
			        port, database, dbusername, dbpassword);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
        
        Accessioner accessioner = new Accessioner(ds);
        

		IMSRTabWebSummary summary = null;
		try {
		    summary = IMSRTabWebSummary.getInstance();
        } catch (NumberFormatException e) {
            log.error("Unable to download summary", e);
            System.exit(1);
            return;
        } catch (ParseException e) {
            log.error("Unable to download summary", e);
            System.exit(1);
            return;
        } catch (IOException e) {
            log.error("Unable to download summary", e);
            System.exit(1);
            return;
        }

        IMSRTabWebDownload downloader = new IMSRTabWebDownload();
        for(int i = 0; i < summary.sites.size(); i++){
            String site = summary.sites.get(i);
            String subID = "GMS-"+site;
            File rawFile = new File(new File(outdir, subID), "raw.tab.txt");
            File preFile = new File(new File(outdir, subID), "sampletab.pre.txt");
            Date rawDate = null;
            Date preDate = null;
            
            if (rawFile.exists()) {
                rawDate = new Date(rawFile.lastModified());
            }
            if (preFile.exists()) {
                preDate = new Date(preFile.lastModified());
            }
            
            if (rawDate == null || summary.updates.get(i).after(rawDate)) {
                //get the raw.tab.txt file
                downloader.download(subID, rawFile);
            }
            
            if (preDate == null || summary.updates.get(i).after(preDate)) {
                // convert raw.tab.txt to sampletab.pre.txt
                IMSRTabToSampleTab c = new IMSRTabToSampleTab(accessioner);
                try {
                    c.convert(rawFile, preFile);
                } catch (NumberFormatException e) {
                    log.error("Problem processing "+rawFile, e);
                    return;
                } catch (IOException e) {
                    log.error("Problem processing "+rawFile, e);
                    return;
                } catch (uk.ac.ebi.arrayexpress2.magetab.exception.ParseException e) {
                    log.error("Problem processing "+rawFile, e);
                    return;
                } catch (java.text.ParseException e) {
                    log.error("Problem processing "+rawFile, e);
                    return;
                } catch (RuntimeException e) {
                    log.error("Problem processing "+rawFile, e);
                    return;
                } catch (SQLException e) {
                    log.error("Problem processing "+rawFile, e);
                    return;
                } catch (ClassNotFoundException e) {
                    log.error("Problem processing "+rawFile, e);
                    return;
                }
                //submit to conan for further processing, if needed
                if (!noconan) {
                    submitConan(subID);
                }
            }
        }
        
	}
}
