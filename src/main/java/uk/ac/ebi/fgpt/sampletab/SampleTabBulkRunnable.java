package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.mged.magetab.error.ErrorItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.GroupNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.arrayexpress2.sampletab.validator.SampleTabValidator;

public class SampleTabBulkRunnable implements Callable<Void> {
    private final File sampletabpre;
    private final File sampletab;
    private final File sampletabtoload;
    
    private final Corrector corrector;
    private final CorrectorAddAttr correctorAddAttr;
    private final SameAs sameAs;
    private final DerivedFrom derivedFrom;
    private final Accessioner accessioner;
    private final boolean force;
    private final boolean noload;
    private final boolean nogroup;

    private static final String SUBSEVENT = "SampleTabBulk";
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    public SampleTabBulkRunnable(File subdir, Corrector corrector, CorrectorAddAttr correctorAddAttr, Accessioner accessioner, SameAs sameAs, DerivedFrom derivedFrom, boolean force, boolean noload, boolean nogroup) {
        
        sampletabpre = new File(subdir, "sampletab.pre.txt");
        sampletab = new File(subdir, "sampletab.txt");
        sampletabtoload = new File(subdir, "sampletab.toload.txt");
        
        this.corrector = corrector;
        this.correctorAddAttr = correctorAddAttr;
        this.sameAs = sameAs;
        this.derivedFrom = derivedFrom;
        this.accessioner = accessioner;
        this.force = force;
        this.noload = noload;
        this.nogroup = nogroup;
        
        if (!nogroup && noload) {
        	log.warn("Nonsensical combination of nogroup = False and noload = true");
        }
    }

    @Override
    public Void call() throws Exception {
        
        String accession = sampletabpre.getParentFile().getName();
        
        doWork();
            
        return null;
        
    }
    

    private void doWork() throws Exception {

        // accession sampletab.pre.txt to sampletab.txt
        if (!force && sampletab.exists() && sampletab.length()==0) {
            log.warn("Skipping "+sampletab+" - is zero size");
        } else if (!force && sampletab.exists() && sampletab.lastModified() > sampletabpre.lastModified()) {
            log.trace("Skipping "+sampletab+" - modifed after "+sampletabpre);
        } else {
            log.info("Processing " + sampletab);

            SampleTabSaferParser parser = new SampleTabSaferParser(new SampleTabValidator());
            
            SampleData st;
            try {
                st = parser.parse(sampletabpre);
            } catch (ParseException e) {
                log.error("Problem processing "+sampletabpre, e);
                for (ErrorItem err : e.getErrorItems()){
                    log.error(err.toString());
                }
                throw e;
            }
            
            
            try {
                //previously, we would accession here
                //now we need to accession earlier with a specific username
                //Therefore, check for unaccessioned stuff and report as errors

                for (SampleNode sample : st.scd.getNodes(SampleNode.class)) {
                    if (sample.getSampleAccession() == null) {
                        throw new Exception("Unaccessioned sample "+sample.getNodeName());
                    }
                }
                for (GroupNode group : st.scd.getNodes(GroupNode.class)) {
                    if (group.getGroupAccession() == null) {
                        throw new Exception("Unaccessioned group "+group.getNodeName());
                    }
                }
            } catch (ParseException e) {
                log.error("Problem processing "+sampletabpre, e);
                for (ErrorItem err : e.getErrorItems()){
                    log.error(err.toString());
                }
                throw e;
            } catch (SQLException e) {
                throw e;
            } catch (RuntimeException e){
                throw e;
            }
            
            log.trace("Applying extra attributes...");
            correctorAddAttr.addAttribute(st);
            for (SampleNode sample : st.scd.getNodes(SampleNode.class)) {
                correctorAddAttr.addAttribute(st, sample);
            }

            log.trace("Applying corrections...");
            corrector.correct(st);

            //dont detect relationships for reference samples
            //these will be done manually
            if (!st.msi.submissionReferenceLayer) {
            	if (derivedFrom != null) {
	                log.trace("Detecting derived from...");
	                try {
	                    derivedFrom.convert(st);
	                } catch (IOException e) {
	                    throw e;
	                }
            	}

                log.trace("Detecting same as...");
                if (sameAs != null) {
	                try {
	                    sameAs.convert(st);
	                } catch (IOException e) {
	                    throw e;
	                }
                }
            }
            
            //write it back out
            Writer writer = null;
            try {
                writer = new FileWriter(sampletab);
                SampleTabWriter sampletabwriter = new SampleTabWriter(writer);
                log.trace("created SampleTabWriter");
                sampletabwriter.write(st);
                sampletabwriter.close();
            } catch (IOException e) {
                throw e;
            } finally {
                if (writer != null){
                    try {
                        writer.close();
                    } catch (IOException e2) {
                        //do nothing
                    }
                }
            }
            
        }

        // preprocess to load
        if (!noload) {
            if (!force && sampletabtoload.exists() && sampletabtoload.length()==0) {
                log.warn("Skipping "+sampletabtoload+" - is zero size");
            } else if (!force && sampletabtoload.exists() && sampletabtoload.lastModified() > sampletab.lastModified()) {
                log.trace("Skipping "+sampletabtoload+" - modifed after "+sampletab);
            } else {
                log.info("Processing " + sampletabtoload);

                SampleTabToLoad c;
                try {
                    c = new SampleTabToLoad();
                    c.convert(sampletab, sampletabtoload);
                } catch (ClassNotFoundException e) {
                    log.error("Problem processing "+sampletab, e);
                    throw e;
                } catch (IOException e) {
                    log.error("Problem processing "+sampletab, e);
                    throw e;
                } catch (ParseException e) {
                    log.error("Problem processing "+sampletab, e);
                    throw e;
                } catch (RuntimeException e){
                    log.error("Problem processing "+sampletab, e);
                    throw e;
                } catch (SQLException e) {
                    log.error("Problem processing "+sampletab, e);
                    throw e;
                }
                log.info("Finished " + sampletabtoload);
            }
        }
    }
           
}
