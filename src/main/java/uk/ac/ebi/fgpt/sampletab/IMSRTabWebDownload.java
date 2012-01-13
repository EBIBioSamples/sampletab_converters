package uk.ac.ebi.fgpt.sampletab;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IMSRTabWebDownload {
	// singlton instance
	private static IMSRTabWebDownload instance = null;

	// logging
	private Logger log = LoggerFactory.getLogger(getClass());

	private IMSRTabWebDownload() {
		// private constructor to prevent accidental multiple initialisations

	}

	public static IMSRTabWebDownload getInstance() {
		if (instance == null) {
			instance = new IMSRTabWebDownload();
		}

		return instance;
	}

	public String download(String accession, String outdir) {
		return this.download(accession, new File(outdir));
	}

	public String download(String accession, File outfile) {

		int ident = Integer.parseInt(accession);
		String url = "http://www.findmice.org/fetch?page=imsrReport&report=repository&site="
				+ ident + "&print=data";

		// setup the input as buffered characters
		// setup the output as a buffered file
		BufferedReader input;
		BufferedWriter output;
		String line;
		
		//create parent directories, if they dont exist
		outfile = outfile.getAbsoluteFile();
		if (!outfile.getParentFile().exists()){
			outfile.getParentFile().mkdirs();
		} 

		log.info("Prepared for download.");
		try {
			input = new BufferedReader(new InputStreamReader(
					new URL(url).openStream()));
			output =  new BufferedWriter(new FileWriter(outfile));
			// now go through each line in turn
			log.info("Starting download...");
			while ((line = input.readLine()) != null) {
				if (line.contains("An unknown database error occurred.")){
					throw new IOException("An unknown database error occurred.");
				}
				output.write(line);
				output.write("\n");
			}
			log.info("Download complete.");
			input.close();
			output.close();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out
					.println("Must provide a IMSR site number and an output directory.");
			return;
		}
		String accession = args[0];
		String outdir = args[1];

		IMSRTabWebDownload downloader = IMSRTabWebDownload
				.getInstance();
		String error = downloader.download(accession, outdir);
		if (error != null) {
			System.out.println("ERROR: " + error);
			System.exit(1);
		}

	}

}
