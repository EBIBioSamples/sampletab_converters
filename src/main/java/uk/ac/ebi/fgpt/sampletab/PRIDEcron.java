package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PRIDEcron {
    private Logger log = LoggerFactory.getLogger(getClass());
    // singlton instance
    private static PRIDEcron instance = null;

    private FTPClient ftp = null;

    private PRIDEcron() {
        // private constructor to prevent accidental multiple initialisations
    }

    public static PRIDEcron getInstance() {
        if (instance == null) {
            instance = new PRIDEcron();
        }
        return instance;
    }

    public FTPClient getFTPClient() throws IOException {
        if (ftp == null) {
            ftp = new FTPClient();
            String server = "ftp.ebi.ac.uk";
            int reply;
            ftp.connect(server);
            ftp.login("anonymous", "");

            log.info("Connected to " + server + ".");
            log.info(ftp.getReplyString());

            // After connection attempt, check the reply code to verify success.
            reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
                throw new IOException("Unable to connect to ftp server " + server);
            }
            log.info("connected to FTP");
        }
        return ftp;
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

    private File getTrimFile(File outdir, String accesssion){
        File subdir = new File(outdir, "GPR-"+accesssion);
        File trim = new File(subdir, "trimmed.xml");
        return trim.getAbsoluteFile();
    }
    
    public void run(File outdir) {
        FTPClient ftp;
        FTPFile[] files;
        try {
            ftp = getFTPClient();
            log.info("Getting file listing...");
            files = ftp.listFiles("/pub/databases/pride/");
            log.info("Got file listing");
        } catch (IOException e) {
            System.err.println("Unable to connect to FTP");
            e.printStackTrace();
            return;
        }
        Pattern regex = Pattern.compile("PRIDE_Exp_Complete_Ac_([0-9]+)\\.xml\\.gz");

        Runtime runtime = Runtime.getRuntime();
        ExecutorService pool = Executors.newFixedThreadPool(runtime.availableProcessors());

        for (FTPFile file : files) {
            String filename = file.getName();
            Matcher matcher = regex.matcher(filename);
            if (matcher.matches()) {
                String accession = matcher.group(1);
                File outfile = getTrimFile(outdir, accession);
                //do not overwrite existing files
                if (!outfile.exists()){
                    pool.execute(new PRIDEFTPDownload(accession, outfile, false));
                }
            }
        }

        // run the pool and then close it afterwards
        pool.shutdown();
        try {
            //allow 24h to execute. Rather too much, but meh
            pool.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            log.error("Interuppted awaiting thread pool termination");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Must provide the following paramters:");
            System.err.println("  PRIDE local directory");
            return;
        }
        String path = args[0];
        File outdir = new File(path);

        if (outdir.exists() && !outdir.isDirectory()) {
            System.err.println("Target is not a directory");
            return;
        }

        if (!outdir.exists())
            outdir.mkdirs();

        getInstance().run(outdir);
        //tidy up ftp connection
        getInstance().close();
    }
}
