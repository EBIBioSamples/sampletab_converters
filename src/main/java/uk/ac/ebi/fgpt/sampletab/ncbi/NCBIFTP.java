package uk.ac.ebi.fgpt.sampletab.ncbi;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import oracle.net.aso.g;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;

public class NCBIFTP {


	private SimpleDateFormat ftpDateTimeFormat = new SimpleDateFormat("yyyyMMddHHmmss");
	
	protected FTPClient ftpClient = null;
	
	private Logger log = LoggerFactory.getLogger(getClass());
	
	public NCBIFTP() {
	}

	public void setup(String server) {

		ftpClient = new FTPClient();
		try {
			ftpClient.connect(server);
			ftpClient.login("anonymous", "");
			log.trace("Connected to " + server + ".");
			log.trace(ftpClient.getReplyString());

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
		String ftpModString = ftpClient.getModificationTime(remoteFileName);
		//ftpModString will be like "213 20150706073959" so need to trim the first part
		ftpModString = ftpModString.split(" ")[1];
		ftpModDate = ftpDateTimeFormat.parse(ftpModString);
		
		if (localCopy.exists()) {
			long modTimeLong = localCopy.lastModified();
			Date modTime = new Date(modTimeLong);
			
			log.info("FTP time = "+ftpModString);
			log.info("FTP time = "+ftpModDate);
			log.info("File time = "+modTimeLong);
			log.info("File time = "+modTime);
			
			if (modTime.after(ftpModDate)) {
				download = false;
				log.info("Local copy up-to-date, no download needed");
			}
		}

		if (download) {
			log.info("Local copy out-of-date, download needed");
			
			// if we need to download a copy, do so
			
			//create a local temporary location
			//the move the tempoorary location
			//This is a java 7 thing 
			File localTemp = Files.createTempFile(Paths.get(localCopy.getParentFile().toURI()), "GNC", null).toFile();
			
			FileOutputStream fileoutputstream = null;
			try {
				fileoutputstream = new FileOutputStream(localTemp);
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
			log.info("Downloaded " + remoteFileName+" to "+localTemp);
			
			FileUtils.move(localTemp, localCopy);
			
			log.info("Moved "+localTemp+" to "+localCopy);
		}
		
		//now open a stream for the local version
		return new FileInputStream(localCopy);
	}
}
