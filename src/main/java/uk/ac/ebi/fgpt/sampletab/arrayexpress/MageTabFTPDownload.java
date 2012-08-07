package uk.ac.ebi.fgpt.sampletab.arrayexpress;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

public class MageTabFTPDownload {	
	//singlton instance
	private static MageTabFTPDownload instance = null;
	
    // logging
    private Logger log = LoggerFactory.getLogger(getClass());

    
	private MageTabFTPDownload(){
		//private constructor to prevent accidental multiple initialisations
	}
	 
    public static MageTabFTPDownload getInstance() {
    	if (instance == null){
    		instance = new MageTabFTPDownload();
    	}
        return instance;
    }
	
	public void download(String accession, String outdirname) throws IOException{
		this.download(accession, new File(outdirname));	
	}
	
	public void download(String accession, File outdir) throws IOException{
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
				throw new RuntimeException("FTP connection failed");
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
		} finally {
			if(ftp.isConnected()) {
				try {
					ftp.disconnect();
				} catch(IOException ioe) {
					// do nothing
				}
			}
		}
	}

	public static void main(String[] args) {
        MageTabFTPDownload magetabftpdownload = MageTabFTPDownload.getInstance();
        magetabftpdownload.doMain(args);
	}
    public void doMain(String[] args){
		if (args.length < 2){
			System.out.println("Must provide an ArrayExpress submission identifier and an output directory.");
			return;
		}
		String accession = args[0];
		String outdir = args[1];
		
		try {
            download(accession, outdir);
        } catch (IOException e) {
            log.error("Unable to download "+accession, e);
        }

	}

}
