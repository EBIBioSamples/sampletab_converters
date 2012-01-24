package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTPUtils {

	private static Logger log = LoggerFactory.getLogger(FTPUtils.class);

	public static FTPClient connect(String server) throws IOException {
		return connect(server, "anonymous", "");
	}

	public static FTPClient connect(String server, String username,
			String password) throws IOException {
		FTPClient ftp = new FTPClient();
		int reply;
		ftp.connect(server);
		ftp.login(username, password);

		log.info("Connecting to " + server + " ...");
		log.info(ftp.getReplyString());

		// After connection attempt, check the reply code to verify success.
		reply = ftp.getReplyCode();
		if (!FTPReply.isPositiveCompletion(reply)) {
			ftp.disconnect();
			throw new IOException("Unable to connect to FTP server " + server);
		}
		log.info("Connected to " + server);
		return ftp;
	}

	public static void download(FTPClient ftp, String ftpfilename,
			File outfile) throws IOException {
		BufferedOutputStream fos = null;
		fos = new BufferedOutputStream(new FileOutputStream(outfile));
		ftp.retrieveFile(ftpfilename, fos);
		fos.close();
	}
}
