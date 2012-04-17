package uk.ac.ebi.fgpt.sampletab.imsr;

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

	// logging
	private Logger log = LoggerFactory.getLogger(getClass());

	public IMSRTabWebDownload() {

	}
	
	public void download(String accession, String outdir) {
		this.download(accession, new File(outdir));
	}

	public void download(String accession, File outfile) {

		String code = accession.substring(4);
		//results needs to be a value otherwise it only gets a small number
		String url = "http://www.findmice.org/report.txt?repositories="+code+"&results=1000000";

		// setup the input as buffered characters
		// setup the output as a buffered file
		BufferedReader input = null;
		BufferedWriter output = null;
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
			e.printStackTrace();
	        return;
		} catch (IOException e) {
			e.printStackTrace();
	        return;
		} finally {
		    //clean up file handles
		    if (input != null){
                try {
                    input.close();
                } catch (IOException e) {
                    //do nothing
                }
		    }
            if (output != null){
                try {
                    output.close();
                } catch (IOException e) {
                    //do nothing
                }
            }
		}
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out
					.println("Must provide a IMSR accession and an output filename.");
			return;
		}
		String accession = args[0];
		String outdir = args[1];

		IMSRTabWebDownload downloader = new IMSRTabWebDownload();
		downloader.download(accession, outdir);

	}

}
