package uk.ac.ebi.fgpt.sampletab.emblbank;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.biojava.bio.BioException;
import org.biojava.bio.seq.Feature;
import org.biojava.bio.seq.Sequence;
import org.biojava.bio.seq.SequenceIterator;
import org.biojava.bio.seq.io.SeqIOTools;
import org.biojavax.RankedDocRef;
import org.biojavax.SimpleNamespace;
import org.biojavax.bio.seq.RichSequence;
import org.biojavax.bio.seq.RichSequenceIterator;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;

public class RecordToSampleTab {


    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;
    
    @Option(name = "-i", aliases={"--input"}, usage = "input file or glob")
    private String inputFilename;
    
    // logging
    public Logger log = LoggerFactory.getLogger(getClass());
    
    public static void main(String[] args) {
        new RecordToSampleTab().doMain(args);
    }
    
    private SampleData toSampleData(RichSequence rs){
        SampleData st = new SampleData();
        
        if (rs.getAccession() == null || rs.getAccession().trim().length() == 0){
            throw new IllegalArgumentException("Accession must be non-null");
        }
        st.msi.submissionIdentifier = "GMB-"+rs.getAccession().trim();
        
        
        return st;
    }
    
    private void logFeatures(Feature f){
        log.info(f.toString());
        
        for (Object key : f.getAnnotation().keys()){
            log.info(key.toString()+" : "+f.getAnnotation().getProperty(key).toString());
        }
        
        Iterator<Feature> i = f.features();
        while (i.hasNext()){
            Feature f2 = i.next();
            logFeatures(f2);
        }
    }
    
    public void doMain(String[] args){
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
            parser.printSingleLineUsage(System.err);
            System.err.println();
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
            return;
        }
        
        File inputFile = new File(inputFilename);
        if (!inputFile.exists()){
            throw new IllegalArgumentException("input file must exist ("+inputFile+")");
        }
        
        log.info("reading file "+inputFile);

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
            

            log.info("file read");

            SimpleNamespace ns = new SimpleNamespace("biojava");
 
            // You can use any of the convenience methods found in the BioJava 1.6 API
            RichSequenceIterator rsi = RichSequence.IOTools.readEMBLDNA(reader,ns);
 
            // Since a single file can contain more than one sequence, you need to iterate over
            // rsi to get the information.
            while(rsi.hasNext()){
                RichSequence rs;
                try {
                    rs = rsi.nextRichSequence();
                    log.info(rs.getName());
                    log.info(rs.getTaxon().toString());
                    log.info(rs.getAccession());
                    log.info(rs.getDescription());
                    log.info(rs.getIdentifier());
                    log.info(rs.getURN());
                    
                    for(RankedDocRef doc : rs.getRankedDocRefs()){
                        log.info(doc.toString());
                    }
                    
                    for (Feature f : rs.getFeatureSet()){
                        logFeatures(f);
                    }
                    
                } catch (NoSuchElementException e) {
                    log.error("Unable to find element", e);
                } catch (BioException e) {
                    log.error("Threw an exception", e);
                }
                break;
            }
            
            
        } catch (FileNotFoundException e) {
            log.error("cannot find "+inputFile, e);
        } finally {
            if (reader != null){
                try {
                    reader.close();
                } catch (IOException e) {
                    //do nothing
                }
            }
        }
        
    }

}
