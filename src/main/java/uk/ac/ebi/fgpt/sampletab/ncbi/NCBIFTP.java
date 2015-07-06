package uk.ac.ebi.fgpt.sampletab.ncbi;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

public class NCBIFTP {


	private SimpleDateFormat ftpDateTimeFormat = new SimpleDateFormat("YYYYMMDDhhmmss");
	
	protected FTPClient ftpClient = null;
	
	private Logger log = LoggerFactory.getLogger(getClass());
	
	public NCBIFTP() {
	}

	public void setup(String server) {

		ftpClient = new FTPClient();
		try {
			ftpClient.connect(server);
			ftpClient.login("anonymous", "");
			log.info("Connected to " + server + ".");
			log.info(ftpClient.getReplyString());

			// After connection attempt, check the reply code to verify success.
			int reply = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				ftpClient.disconnect();
				throw new IOException("FTP connection to complete positively");
			}
			
			//make sure we are in binary mode
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			
		} catch (SocketException e) {
			log.error("Unable to connect to "+server, e);
			return;
		} catch (IOException e) {
			log.error("Unable to connect to "+server, e);
			return;
		}
	}
	/**
	 * Will try to return an inputstream from the server for the remote file name.
	 * 
	 * Note: ensure that this stream is closed in a finally block to prevent leakage.
	 * 
	 * Note: the setup() function must have been called first to establish a connection 
	 * to the appropriate server.
	 * 
	 * @param remoteFileName
	 * @return
	 * @throws IOException 
	 */
	public InputStream streamFromFTP(String remoteFileName) throws IOException {
		return ftpClient.retrieveFileStream(remoteFileName);
	}
	
	/**
	 * Will check if the local copy exists and is newer than that remote. If not,
	 * will download the remote into the local copy.
	 * 
	 * Then returns an InputStream from that local copy for further processing.
	 * 
	 * Note: ensure that this stream is closed in a finally block to prevent leakage.
	 * 
	 * Note: the setup() function must have been called first to establish a connection 
	 * to the appropriate server.
	 * 
	 * @param remoteFileName
	 * @param localCopy
	 * @return
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public InputStream streamFromLocalCopy(String remoteFileName, File localCopy) throws ParseException, IOException {

		boolean download = true;
		Date ftpModDate = null;
		ftpModDate = ftpDateTimeFormat.parse(ftpClient.getModificationTime(remoteFileName));
		
		if (localCopy.exists()) {
			Date modTime = new Date(localCopy.lastModified());
			if (modTime.after(ftpModDate)) {
				download = false;
				log.info("Local copy up-to-date, no download needed");
			}
		}

		if (download) {
			log.info("Local copy out-of-date, no download needed");
			
			// if we need to download a copy, do so
			FileOutputStream fileoutputstream = null;
			try {
				fileoutputstream = new FileOutputStream(localCopy);
				ftpClient.retrieveFile(remoteFileName, fileoutputstream);
			} finally {
				if(fileoutputstream != null) {
					try {
						fileoutputstream.close();
					} catch (IOException e) {
						//do nothing
					}
				}
			}
			log.info("Downloaded " + remoteFileName+" to "+localCopy);
		}
		
		//now open a stream for the local version
		return new FileInputStream(localCopy);
	}
}
