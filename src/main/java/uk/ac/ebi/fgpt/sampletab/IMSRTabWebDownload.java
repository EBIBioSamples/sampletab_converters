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

	// logging
	private Logger log = LoggerFactory.getLogger(getClass());

	public IMSRTabWebDownload() {

	}
	
	public int getAccessionID(String accession){
        //IMSR web interface takes a number, not the site code
        //However, this is not a straightforward lookup on the summary.
        //For the moment, it is hardcoded here.
        //TODO unhardcode this
        int accessionid = 0;
        /*
        GMS-JAX
        GMS-HAR
        GMS-MMRRC
        GMS-ORNL
        GMS-CARD
        GMS-EM
        GMS-NMICE
        GMS-RBRC
        GMS-NCIMR
        GMS-CMMR
        GMS-APB
        GMS-EMS
        GMS-HLB
        GMS-NIG
        GMS-TAC
        GMS-MUGEN
        GMS-TIGM
        GMS-KOMP
        GMS-RMRC-NLAC
        GMS-OBS
        GMS-WTSI
         */
        
        if      (accession.equals("GMS-JAX")) accessionid = 1;
        else if (accession.equals("GMS-HAR")) accessionid = 2;
        else if (accession.equals("GMS-MMRRC")) accessionid = 3;
        else if (accession.equals("GMS-ORNL")) accessionid = 4;
        else if (accession.equals("GMS-CARD")) accessionid = 5;
        else if (accession.equals("GMS-EM")) accessionid = 6;
        else if (accession.equals("GMS-NMICE")) accessionid = 7;
        else if (accession.equals("GMS-RBRC")) accessionid = 9;
        else if (accession.equals("GMS-NCIMR")) accessionid = 10;
        else if (accession.equals("GMS-CMMR")) accessionid = 11;
        else if (accession.equals("GMS-APB")) accessionid = 12;
        else if (accession.equals("GMS-EMS")) accessionid = 13;
        else if (accession.equals("GMS-HLB")) accessionid = 14;
        else if (accession.equals("GMS-NIG")) accessionid = 17;
        else if (accession.equals("GMS-TAC")) accessionid = 20;
        else if (accession.equals("GMS-MUGEN")) accessionid = 21;
        else if (accession.equals("GMS-TIGM")) accessionid = 22; //This is the really big one
        else if (accession.equals("GMS-KOMP")) accessionid = 23;
        else if (accession.equals("GMS-RMRC-NLAC")) accessionid = 24;
        else if (accession.equals("GMS-OBS")) accessionid = 25;
        else if (accession.equals("GMS-WTSI")) accessionid = 26;
    
        return accessionid;
	}

	public void download(String accession, String outdir) {
		this.download(accession, new File(outdir));
	}

	public void download(String accession, File outfile) {

		int ident = getAccessionID(accession);
		String url = "http://www.findmice.org/fetch?page=imsrReport&report=repository&site="
				+ ident + "&print=data";

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
