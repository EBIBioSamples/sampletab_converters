package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.utils.FTPUtils;

public class IMSRcron {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Option(name = "-o", aliases={"--output"}, usage = "output directory")
    private String outputDirName;
        
	private Logger log = LoggerFactory.getLogger(getClass());

	private IMSRcron() {
	}
	

	public static void main(String[] args) {
        new IMSRcron().doMain(args);
    }

    public void doMain(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            // parse the arguments.
            parser.parseArgument(args);
            // TODO check for extra arguments?
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            help = true;
        }

        if (help) {
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
            return;
        }
        
		File outdir = new File(this.outputDirName);

		if (outdir.exists() && !outdir.isDirectory()) {
			System.err.println("Target is not a directory");
			System.exit(1);
			return;
		}

		if (!outdir.exists())
			outdir.mkdirs();


        IMSRTabWebDownload downloader = new IMSRTabWebDownload();
        downloader.download("GMS-JAX", new File(new File(outdir, "GMS-JAX"), "raw.tab.txt"));
        downloader.download("GMS-HAR", new File(new File(outdir, "GMS-HAR"), "raw.tab.txt"));
        downloader.download("GMS-MMRRC", new File(new File(outdir, "GMS-MMRRC"), "raw.tab.txt"));
        downloader.download("GMS-ORNL", new File(new File(outdir, "GMS-ORNL"), "raw.tab.txt"));
        downloader.download("GMS-CARD", new File(new File(outdir, "GMS-CARD"), "raw.tab.txt"));
        downloader.download("GMS-EM", new File(new File(outdir, "GMS-EM"), "raw.tab.txt"));
        downloader.download("GMS-NMICE", new File(new File(outdir, "GMS-NMICE"), "raw.tab.txt"));
        downloader.download("GMS-RBRC", new File(new File(outdir, "GMS-RBRC"), "raw.tab.txt"));
        downloader.download("GMS-NCIMR", new File(new File(outdir, "GMS-NCIMR"), "raw.tab.txt"));
        downloader.download("GMS-CMMR", new File(new File(outdir, "GMS-CMMR"), "raw.tab.txt"));
        downloader.download("GMS-APB", new File(new File(outdir, "GMS-APB"), "raw.tab.txt"));
        downloader.download("GMS-EMS", new File(new File(outdir, "GMS-EMS"), "raw.tab.txt"));
        downloader.download("GMS-HLB", new File(new File(outdir, "GMS-HLB"), "raw.tab.txt"));
        downloader.download("GMS-NIG", new File(new File(outdir, "GMS-NIG"), "raw.tab.txt"));
        downloader.download("GMS-TAC", new File(new File(outdir, "GMS-TAC"), "raw.tab.txt"));
        downloader.download("GMS-MUGEN", new File(new File(outdir, "GMS-MUGEN"), "raw.tab.txt"));
        downloader.download("GMS-TIGM", new File(new File(outdir, "GMS-TIGM"), "raw.tab.txt"));
        downloader.download("GMS-KOMP", new File(new File(outdir, "GMS-KOMP"), "raw.tab.txt"));
        downloader.download("GMS-RMRC-NLAC", new File(new File(outdir, "GMS-RMRC-NLAC"), "raw.tab.txt"));
        downloader.download("GMS-OBS", new File(new File(outdir, "GMS-OBS"), "raw.tab.txt"));
        downloader.download("GMS-WTSI", new File(new File(outdir, "GMS-WTSI"), "raw.tab.txt"));
        
	}
}
