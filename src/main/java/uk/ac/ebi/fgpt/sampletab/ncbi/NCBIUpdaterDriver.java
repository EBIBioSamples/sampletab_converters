package uk.ac.ebi.fgpt.sampletab.ncbi;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.AbstractDriver;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class NCBIUpdaterDriver extends AbstractDriver {

    @Argument(required=true, index=0, metaVar="FROM", usage = "from date (yyyy/MM/dd)")
    protected String fromDateString;

    @Argument(required=true, index=1, metaVar="TO", usage = "to date (yyyy/MM/dd)")
    protected String toDateString;

    @Option(name = "-o", aliases={"--output"}, metaVar="OUTPUT", usage = "directory to write to")
    protected File outDir = null;

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");

    private NCBIUpdateDownloader downloader = new NCBIUpdateDownloader();

    private Logger log = LoggerFactory.getLogger(getClass());
    
    
    protected void handleId(int id) {
        Document document = null;
        try {
            document = downloader.getId(id);
        } catch (MalformedURLException e) {
            log.error("Problem retrieving BioSample "+id, e);
            return;
        } catch (DocumentException e) {
            log.error("Problem retrieving BioSample "+id, e);
            return;
        } catch (IOException e) {
            log.error("Problem retrieving BioSample "+id, e);
            return;
        }
        
        Element sampleSet = document.getRootElement();
        Element sample = XMLUtils.getChildByName(sampleSet, "BioSample");
        String accession = sample.attributeValue("accession");
        
        if (accession == null) {
            log.warn("No accession for sample "+id);
            return;
        }
        
        File localOutDir = new File(outDir, SampleTabUtils.getSubmissionDirPath("GNC-"+accession));
        localOutDir = localOutDir.getAbsoluteFile();
        localOutDir.mkdirs();
        File outFile = new File(localOutDir, "out.xml");

        log.info("writing to "+outFile);
        
        try {
            XMLUtils.writeDocumentToFile(document, outFile);
        } catch (IOException e) {
            log.error("Problem writing to "+outFile, e);
            return;
        }
    }
    
    protected void doMain(String[] args) {
        super.doMain(args);

        Date fromDate = null;
        Date toDate = null;
        try {
            fromDate = dateFormat.parse(fromDateString);
            toDate = dateFormat.parse(toDateString);
        } catch (ParseException e) {
            log.error("Unable to parse date");
            return;
        }
                
        try {
            for(int id : downloader.getUpdatedSampleIds(fromDate, toDate)) {
                log.info("getting id "+id);
                handleId(id);
            }
        } catch (MalformedURLException e) {
            log.error("Problem retrieving BioSamples updated list", e);
        } catch (DocumentException e) {
            log.error("Problem retrieving BioSamples updated list", e);
        } catch (IOException e) {
            log.error("Problem retrieving BioSamples updated list", e);
        }
    }
    
    public static void main(String[] args){
        new NCBIUpdaterDriver().doMain(args);
    }
}
