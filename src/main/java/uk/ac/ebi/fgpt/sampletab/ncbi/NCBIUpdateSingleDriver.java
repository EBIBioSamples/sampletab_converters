package uk.ac.ebi.fgpt.sampletab.ncbi;

import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.AbstractDriver;

public class NCBIUpdateSingleDriver extends AbstractDriver{

    @Argument(required=true, index=0, metaVar="OUTPUT", usage = "output directory")
    protected File outDir;
    
    @Argument(required=true, index=1, metaVar="SUBMISSION", usage = "submission ID(s)")
    protected List<String> submissionIds;
    
    @Option(name = "--no-conan", usage = "do not trigger conan loads?")
    private boolean noconan = false;

	@Option(name = "--force", aliases = { "-f" }, usage = "force updates")
	protected boolean force = false;

    private Logger log = LoggerFactory.getLogger(getClass());
    	
    protected void doMain(String[] args) {
        super.doMain(args);

        for (String submissionId : submissionIds) {
	        if (!submissionId.startsWith("GNC-SAM") || submissionId.startsWith("GNC-SAME")) {
	        	log.warn("Submission ID must be an NCBI accession starting with GNC-SAM not GNC-SAME ("+submissionId+")");
	        	continue;
	        }
	        
	        int id = Integer.parseInt(submissionId.substring(8, submissionId.length()));        
	        
	        Callable<Void> call = new NCBIUpdateDownloader.DownloadConvertCallable(id, outDir, !noconan, force);
			try {
				call.call();
			} catch (Exception e) {
				log.error("Problem processing "+submissionId, e);
			}
        }
    }
    
    public static void main(String[] args){
        new NCBIUpdateSingleDriver().doMain(args);
    }

}
