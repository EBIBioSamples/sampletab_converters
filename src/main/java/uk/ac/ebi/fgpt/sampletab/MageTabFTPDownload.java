package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

public class MageTabFTPDownload {	
	//singlton instance
	private static final MageTabFTPDownload instance = new MageTabFTPDownload();
	
    // logging
    private Logger log = LoggerFactory.getLogger(getClass());

	private MageTabFTPDownload(){
		//private constructor to prevent accidental multiple initialisations
	}
	 
    public static MageTabFTPDownload getInstance() {
        return instance;
    }
	
	public String download(String accession, String outdir){
		return this.download(accession, new File(outdir));	
	}
	
	public String download(String accession, File outdir){
		boolean error = false;
		FTPClient ftp = new FTPClient();
		String server = "ftp.ebi.ac.uk";
		try {
			int reply;
			ftp.connect(server);
			ftp.login("anonymous", "");
			log.info("Connected to " + server + ".");
			log.info(ftp.getReplyString());

			// After connection attempt, check the reply code to verify success.
			reply = ftp.getReplyCode();
			if(!FTPReply.isPositiveCompletion(reply)) {
				ftp.disconnect();
				return "FTP server refused connection.";
			}
			
			//Now we can work out the paths we need
			String subdirname = accession.substring(2); //strip the E- from start
			subdirname = subdirname.substring(0, subdirname.indexOf("-"));

			String path = "pub/databases/arrayexpress/data/experiment/"+subdirname+"/"+accession+"/";
			
			//construct directories if required
			outdir.mkdirs();

			//actually get the files
			File idffile = new File(outdir, accession+".idf.txt");
			FileOutputStream idffileoutputstream = new FileOutputStream(idffile);
			ftp.retrieveFile(path+accession+".idf.txt", idffileoutputstream);
			log.info("Downloaded " + path+accession+".idf.txt");
			idffileoutputstream.close();

			File sdrffile = new File(outdir, accession+".sdrf.txt");
			FileOutputStream sdrffileoutputstream = new FileOutputStream(sdrffile);
			ftp.retrieveFile(path+accession+".sdrf.txt", sdrffileoutputstream);
			log.info("Downloaded " + path+accession+".sdrf.txt");
			sdrffileoutputstream.close();
			
			/*
			//this could be done using paged access. At the moment, bulk access is good enough.
			ftp.changeWorkingDirectory("pub/databases/arrayexpress/data/experiment/");
			for (FTPFile subdir: ftp.listDirectories()){
				ftp.changeWorkingDirectory(subdir.getName());
				for (FTPFile subsubdir: ftp.listDirectories()){
					System.out.println(subsubdir.toFormattedString());
				}
				ftp.changeToParentDirectory();
			}
			*/

			ftp.logout();
		} catch(IOException e) {
			error = true;
			e.printStackTrace();
		} catch(RuntimeException e) {
			error = true;
			e.printStackTrace();
		} finally {
			if(ftp.isConnected()) {
				try {
					ftp.disconnect();
				} catch(IOException ioe) {
					// do nothing
				}
			}
			if (error){
				return "An IOException error occored";
			}
		}
		//return a null string to indicate nothing went wrong
		return null;
	}

	public static void main(String[] args) {
		String accession = args[1];
		String outdir = args[2];
		
		MageTabFTPDownload magetabftpdownload = MageTabFTPDownload.getInstance();
		String error = magetabftpdownload.download(accession, outdir);
		if (error != null){
			System.out.println("ERROR: "+error);
			System.exit(1);
		}

	}

}
