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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.FTPUtils;

public class MageTabcron {
	private Logger log = LoggerFactory.getLogger(getClass());
	// singlton instance
	private static MageTabcron instance = null;

	private FTPClient ftp = null;

	private MageTabcron() {
		// private constructor to prevent accidental multiple initialisations
	}

	public static MageTabcron getInstance() {
		if (instance == null) {
			instance = new MageTabcron();
		}
		return instance;
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
        ExecutorService pool = Executors.newFixedThreadPool(nothreads*2);
        
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

						log.info("working on " + subsubdirpath);

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
	                    pool.execute(new CurlDownload("ftp://ftp.ebi.ac.uk"+idfpath, outidf.getAbsolutePath()));
	                    pool.execute(new CurlDownload("ftp://ftp.ebi.ac.uk"+sdrfpath, outsdrf.getAbsolutePath()));
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
		if (args.length < 1) {
			System.err.println("Must provide the following paramters:");
			System.err.println("  ArrayExpress local directory");
			System.exit(1);
			return;
		}
		String path = args[0];
		File outdir = new File(path);

		if (outdir.exists() && !outdir.isDirectory()) {
			System.err.println("Target is not a directory");
			System.exit(1);
			return;
		}

		if (!outdir.exists())
			outdir.mkdirs();

		getInstance().run(outdir);
		// tidy up ftp connection
		getInstance().close();
	}
}
