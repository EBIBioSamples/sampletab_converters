package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PRIDEFTPDownload implements Runnable {

    // logging
    private Logger log = LoggerFactory.getLogger(getClass());

    
    private String accession = null;
    private File outfile = null;

    public PRIDEFTPDownload() {
        
    }
    
    public PRIDEFTPDownload(String accession, String outfilename) {
        this.accession = accession;
        this.outfile = new File(outfilename);
    }
    
    public PRIDEFTPDownload(String accession, File outfile) {
        this.accession = accession;
        this.outfile = outfile;
    }

    public static PRIDEFTPDownload getInstance() {
        return new PRIDEFTPDownload();
    }


    public boolean download(String accession, String outfilename) {
        return this.download(accession, new File(outfilename));
    }

    public boolean download(String accession, File outfile) {

        //make sure the path is valid
        // construct directories if required
        outfile = outfile.getAbsoluteFile();
        File outdir = outfile.getParentFile();
        if (!outdir.exists()){
            outdir.mkdirs();
        }

        // for some reason that escapes me, the standard java ftp /gunzip tools dont work reliably for PRIDE data
        // therefore we sacrifice multiplatformness to run well
        List<String> command = new ArrayList<String>();
        //command.add("wget");
        //command.add("-O");
        command.add("curl");
        command.add("-o");
        command.add(outfile + ".gz");
        command.add("ftp://ftp.ebi.ac.uk/pub/databases/pride/PRIDE_Exp_Complete_Ac_" + accession + ".xml.gz");

        //create the actual process that will do the download
        ProcessBuilder pb = new ProcessBuilder();
        pb.command(command);
        Process p;

        //do the download
        try {
            p = pb.start();
            synchronized (p) {
                p.wait();
            }
        } catch (IOException e) {
            log.error("Unable to run "+command.get(0));
            e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            log.error("Unable to run "+command.get(0));
            e.printStackTrace();
            return false;
        }

        log.info("Downloaded " + outfile + ".gz");

        //now we need to extract & filter the file
        try {
            String bashcom = "gunzip -c -d -f "
                    + outfile + ".gz "
                    + "| sed '/<GelFreeIdentification>/,/<\\/GelFreeIdentification>/d' "
                    + "| sed '/<TwoDimensionalIdentification>/,/<\\/TwoDimensionalIdentification>/d'" 
                    + "| sed '/<spectrumList count=/,/<\\/spectrumList>/d'" 
                    + " > "+outfile;
            log.info(bashcom);

            command = new ArrayList<String>();
            command.add("/bin/sh");
            command.add("-c");
            command.add(bashcom);
            pb.command(command);
            
            p = pb.start();
            synchronized (p) {
                p.wait();
            }

            log.info("Trimmed " + outfile);

        } catch (IOException e) {
            log.error("Unable to run bash");
            e.printStackTrace();
            return false;
        } catch (RuntimeException e) {
            log.error("Unable to run bash");
            e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            log.error("Unable to run bash");
            e.printStackTrace();
            return false;
        }
        
        //clean up by deleting gzip version
        File gz = new File(outfile+".gz");
        gz.delete();
        log.info("Cleaned up file "+gz);
        return true;

    }

    public void run() {
        download(this.accession, this.outfile);
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Must provide an PRIDE identifier and an output filename.");
            return;
        }
        String accession = args[0];
        String outdir = args[1];

        PRIDEFTPDownload prideftpdownload = new PRIDEFTPDownload();
        prideftpdownload.download(accession, outdir);
    }

}
