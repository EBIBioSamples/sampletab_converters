package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.FTPUtils;

public class DGVaXMLcron {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Option(name = "-o", aliases={"--output"}, usage = "output directory")
    private String outputDirName;
    
    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;
    
	private Logger log = LoggerFactory.getLogger(getClass());

	private FTPClient ftp = null;

	private DGVaXMLcron() {
	}

	private void close() {
		try {
			ftp.logout();
		} catch (IOException e) {
			if (ftp.isConnected()) {
				try {
					ftp.disconnect();
				} catch (IOException ioe) {
					// do nothing
				}
			}
		}

	}
	
	private class CurlDownload implements Runnable {
		private final String url;
		private final String filename;
        private final String username;
        private final String password;
		
        public CurlDownload(String url, String filename, String username, String password){
            this.url = url;
            this.filename = filename;
            this.username = username;
            this.password = password;
        }
        
        public void run(){
	        ProcessBuilder pb = new ProcessBuilder();
	        Process p;
	        String bashcom;
	        ArrayList<String> command;
	        
	        log.info("Curl downloading "+this.filename);
	        
	        bashcom = "curl -z "+filename+" -o "+filename+" -u "+username+":"+password+" "+url;
	        log.debug(bashcom);

            command = new ArrayList<String>();
            command.add("/bin/bash");
            command.add("-c");
            command.add(bashcom);
            pb.command(command);
            
            try {
				p = pb.start();
	            synchronized (p) {
	                p.waitFor();
	            }
			} catch (IOException e) {
				System.err.println("Error running "+bashcom);
				e.printStackTrace();
				return;
			} catch (InterruptedException e) {
				System.err.println("Error running "+bashcom);
				e.printStackTrace();
				return;
			}
        	
        }
	}

	public void run(File outdir) {
	    String server = "ftp-private.ebi.ac.uk";
	    String username = "ega-box-102";
	    String password = "IoXdA4XC";
		try {
			ftp = FTPUtils.connect(server, username, password);
		} catch (IOException e) {
		    log.error("Unable to connect to FTP");
			e.printStackTrace();
			System.exit(1);
			return;
		}

        FTPFile[] ftpfiles = null;
		try {
			ftpfiles = ftp.listFiles();
		} catch (IOException e) {
		    log.error("Unable to connect to FTP");
			e.printStackTrace();
		}


        int nothreads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(nothreads);
        
		if (ftpfiles != null) {
			//convert the subdir FTPFile objects to string names
			//otherwise the time it takes to process GEOD causes problems.
			Collection<String> subdirstrs = new ArrayList<String>();
			for (FTPFile ftpfile : ftpfiles) {
				log.info("working on " + ftpfile);

				String subid = ftpfile.getName().split("_")[0];
				if (subid.equals("estd59")){
				    //this is a huge multi-file submission
				    //for the moment, skip it
				    log.info("Skipping estd59");
				    continue;
				}
				if (!subid.matches("[en]std[0-9]+")){
				    //not a submission, skip it
                    log.info("Skipping "+subid);
				    continue;
				}
                        
				File outsubdir = new File(outdir, "GVA-"
						+ subid);
				if (!outsubdir.exists())
					outsubdir.mkdirs();
			
				File outstudy = new File(outsubdir, "raw.xml");
				
                Calendar ftptime = ftpfile.getTimestamp();
                Calendar outtime = new GregorianCalendar();
                outtime.setTimeInMillis(outstudy.lastModified());

                //rather than using the java FTP libraries - which seem to
                //break quite often - use curl. Sacrifices multiplatformness
                //for reliability.
				if (!outstudy.exists() || ftptime.after(outtime)){
				    String url = "ftp://"+server+"/"+ftpfile.getName();
                    Runnable t = new CurlDownload(url, outstudy.getAbsolutePath(), username, password);
                    if (threaded){
                        pool.execute(t);
                    } else {
                        t.run();
                    }
				}
			}
		}
		
        // run the pool and then close it afterwards
        // must synchronize on the pool object
        synchronized (pool) {
            pool.shutdown();
            try {
                // allow 24h to execute. Rather too much, but meh
                pool.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                log.error("Interuppted awaiting thread pool termination");
                e.printStackTrace();
            }
        }

        
		// TODO hide files that have disappeared from the FTP site.
	}

	public static void main(String[] args) {
        new DGVaXMLcron().doMain(args);
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

		run(outdir);
        
        // tidy up ftp connection
        close();
	}
}
