package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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

public class MageTabcron {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Option(name = "-o", aliases={"--output"}, usage = "output directory")
    private String outputDirName;
    
    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;
    
	private Logger log = LoggerFactory.getLogger(getClass());

	private FTPClient ftp = null;

	private MageTabcron() {
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
		
        public CurlDownload(String url, String filename){
            this.url = url;
            this.filename = filename;
        }
        
        public void run(){
	        ProcessBuilder pb = new ProcessBuilder();
	        Process p;
	        String bashcom;
	        ArrayList<String> command;
	        
	        log.info("Curl downloading "+this.filename);
	        
	        bashcom = "curl -z "+filename+" -o "+filename+" "+url;
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
		FTPFile[] subdirs = null;
		try {
			ftp = FTPUtils.connect("ftp.ebi.ac.uk");
		} catch (IOException e) {
			System.err.println("Unable to connect to FTP");
			e.printStackTrace();
			System.exit(1);
			return;
		}

		String root = "/pub/databases/arrayexpress/data/experiment/";
		try {
			subdirs = ftp.listDirectories(root);
		} catch (IOException e) {
			System.err.println("Unable to connect to FTP");
			e.printStackTrace();
		}


        int nothreads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(nothreads);
        
		if (subdirs != null) {
			//convert the subdir FTPFile objects to string names
			//otherwise the time it takes to process GEOD causes problems.
			Collection<String> subdirstrs = new ArrayList<String>();
			for (FTPFile subdir : subdirs) {
				subdirstrs.add(subdir.getName());
			}
			for (String subdirstr : subdirstrs) {
				String subdirpath = root + subdirstr + "/";
				log.info("working on " + subdirpath);

				FTPFile[] subsubdirs = null;
				try {
					subsubdirs = ftp.listDirectories(subdirpath);
				} catch (IOException e) {
					System.err.println("Unable to list subdirs " + subdirpath);
					e.printStackTrace();
				}
				if (subsubdirs != null) {
					for (FTPFile subsubdir : subsubdirs) {
						String subsubdirpath = subdirpath + subsubdir.getName()
								+ "/";
						String idfpath = subsubdirpath + subsubdir.getName()
								+ ".idf.txt";
						String sdrfpath = subsubdirpath + subsubdir.getName()
								+ ".sdrf.txt";

						File outsubdir = new File(outdir, "GA"
								+ subsubdir.getName());
						if (!outsubdir.exists())
							outsubdir.mkdirs();
						File outidf = new File(outsubdir, subsubdir.getName()
								+ ".idf.txt");
						File outsdrf = new File(outsubdir, subsubdir.getName()
								+ ".sdrf.txt");

						//rather than using the java FTP libraries - which seem to
						//break quite often - use curl. Sacrifices multiplatformness
						//for reliability.
						Runnable t1 = new CurlDownload("ftp://ftp.ebi.ac.uk"+idfpath, outidf.getAbsolutePath());
                        Runnable t2 = new CurlDownload("ftp://ftp.ebi.ac.uk"+sdrfpath, outsdrf.getAbsolutePath());
						if (threaded){
    	                    pool.execute(t1);
    	                    pool.execute(t2);
						} else {
						    t1.run();
						    t2.run();
						}
					}
				}
				//restart the connection after each subdir
				//otherwise we hit some sort of limit?
//				try {
//					ftp = FTPUtils.connect("ftp.ebi.ac.uk");
//				} catch (IOException e) {
//					System.err.println("Unable to connect to FTP");
//					e.printStackTrace();
//					System.exit(1);
//					return;
//				}
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
        new MageTabcron().doMain(args);
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

		this.run(outdir);
		// tidy up ftp connection
		this.close();
	}
}
