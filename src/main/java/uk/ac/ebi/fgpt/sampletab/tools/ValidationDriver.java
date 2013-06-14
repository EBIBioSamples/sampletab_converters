package uk.ac.ebi.fgpt.sampletab.tools;

import java.io.File;

import org.mged.magetab.error.ErrorItem;
import org.mged.magetab.error.ErrorItemImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.exception.ValidateException;
import uk.ac.ebi.arrayexpress2.magetab.listener.ErrorItemListener;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Publication;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabParser;
import uk.ac.ebi.arrayexpress2.sampletab.validator.SampleTabValidator;
import uk.ac.ebi.fgpt.sampletab.AbstractInfileDriver;

public class ValidationDriver extends AbstractInfileDriver {    
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    @Override
    protected Runnable getNewTask(File inputFile) {
        return new ValidationRunnable(inputFile);
    }
    
    private class ValidationRunnable implements Runnable {

        private final File inputFile;
        private final SampleTabParser<SampleData> parser = new SampleTabParser<SampleData>(new ImprovedSampleTabValidator());
        
        public ValidationRunnable(File inputFileTemp) {
            this.inputFile = inputFileTemp;
            
            parser.addErrorItemListener(new ErrorItemListener() {
                public void errorOccurred(ErrorItem item) {
                    log.error("\t"+inputFile+"\t"+item.getMesg());
                }
            });
        }
        
        @Override
        public void run() {
            SampleData sampledata = null;
            try {
                sampledata = parser.parse(inputFile);
            } catch (ParseException e) {
                log.error("Error when parsing "+inputFile, e);
                throw new RuntimeException(e);
            }
        }
        
    }
    
    private class ImprovedSampleTabValidator  extends SampleTabValidator {

        public synchronized void validate(SampleData sampledata) throws ValidateException {
            super.validate(sampledata);
            
            //some basic errors...
            if (sampledata.msi.submissionTitle == null){
                fireErrorItemEvent(new ErrorItemImpl("Submission Title is null", -1, getClass().getName()));
            } else if (sampledata.msi.submissionTitle.length() < 10){
                fireErrorItemEvent(new ErrorItemImpl("Submission Title is under 10 characters long", -1, getClass().getName()));
            }

            if (sampledata.msi.submissionDescription == null){
                fireErrorItemEvent(new ErrorItemImpl("Submission Description is null", -1, getClass().getName()));
            } else if (sampledata.msi.submissionDescription.length() < 10){
                fireErrorItemEvent(new ErrorItemImpl("Submission Description is under 10 characters long", -1, getClass().getName()));
            }

            if (!sampledata.msi.submissionIdentifier.startsWith("G")) {
                fireErrorItemEvent(new ErrorItemImpl("Submission Identifier does not start with G", -1, getClass().getName()));
            }
            if (sampledata.msi.submissionIdentifier.length() < 3) {
                fireErrorItemEvent(new ErrorItemImpl("Submission Identifier is under 3 characters long", -1, getClass().getName()));
            }
            
            for (Publication p : sampledata.msi.publications) {
                Integer i = null;
                try {
                    i = Integer.parseInt(p.getPubMedID());
                } catch (NumberFormatException e) {
                    fireErrorItemEvent(new ErrorItemImpl("PubMed ID is not a number '"+p.getPubMedID()+"'", -1, getClass().getName()));
                }
                if (i != null) {
                    if (!p.getPubMedID().equals(i.toString())) {
                        fireErrorItemEvent(new ErrorItemImpl("PubMed ID is not a number "+p.getPubMedID(), -1, getClass().getName()));
                    }
                }
            }
            
            
            for (SampleNode s : sampledata.scd.getNodes(SampleNode.class)) {
                
                //if the node has no descriptions, check none of the attributes contain the string "description"
                if (s.getSampleDescription() == null || s.getSampleDescription().length() == 0) {
                    for (SCDNodeAttribute a : s.getAttributes()) {
                        
                        boolean isCommentAttribute = false;
                        synchronized(CommentAttribute.class){
                            isCommentAttribute = CommentAttribute.class.isInstance(a);
                        }
                        boolean isCharacteristicAttribute = false;
                        synchronized(CharacteristicAttribute.class){
                            isCharacteristicAttribute = CharacteristicAttribute.class.isInstance(a);
                        }
                        boolean isOrganism = false;
                        synchronized(OrganismAttribute.class) {
                            isOrganism = OrganismAttribute.class.isInstance(a);
                        }
                        
                        if (isCommentAttribute) {
                            CommentAttribute ca = (CommentAttribute) a;
                            if (ca.type.toLowerCase().equals("sample description") || 
                                    ca.type.toLowerCase().equals("sample_description") || 
                                    ca.type.toLowerCase().equals("description") ) {
                                fireErrorItemEvent(new ErrorItemImpl("Sample "+s.getNodeName()+" has only secondary description", -1, getClass().getName()));
                            }
                        }
                        
                        if (isCharacteristicAttribute) {
                            CharacteristicAttribute ca = (CharacteristicAttribute) a;
                            if (ca.type.toLowerCase().equals("sample description") || 
                                    ca.type.toLowerCase().equals("sample_description") || 
                                    ca.type.toLowerCase().equals("description") ) {
                                fireErrorItemEvent(new ErrorItemImpl("Sample "+s.getNodeName()+" has only secondary description", -1, getClass().getName()));
                            }
                        }
                    }
                }
                
                boolean hasOrganism = false;
                for (SCDNodeAttribute a : s.getAttributes()) {
                    boolean isOrganism = false;
                    synchronized(OrganismAttribute.class) {
                        isOrganism = OrganismAttribute.class.isInstance(a);
                    }
                    boolean isCommentAttribute = false;
                    synchronized(CommentAttribute.class){
                        isCommentAttribute = CommentAttribute.class.isInstance(a);
                    }
                    boolean isCharacteristicAttribute = false;
                    synchronized(CharacteristicAttribute.class){
                        isCharacteristicAttribute = CharacteristicAttribute.class.isInstance(a);
                    }
                    
                    //check that organisms have a tax id
                    if (isOrganism) {
                        hasOrganism = true;
                        OrganismAttribute oa = (OrganismAttribute) a;
                        if (oa.getTermSourceID() == null || oa.getTermSourceID().length() == 0) {
                            fireErrorItemEvent(new ErrorItemImpl("Sample "+s.getNodeName()+" has no Organism Term Source REF for "+oa.getAttributeValue(), -1, getClass().getName()));
                        }
                    }
                    
                    //check that comments and characteristics don't have both units and term source refs
                    if (isCommentAttribute) {
                        CommentAttribute ca = (CommentAttribute) a;
                        if (ca.unit != null && ca.getTermSourceREF() != null) {
                            fireErrorItemEvent(new ErrorItemImpl("Sample "+s.getNodeName()+" has an attribute with both unit and term source", -1, getClass().getName()));
                        }
                    }
                    if (isCharacteristicAttribute) {
                        CharacteristicAttribute ca = (CharacteristicAttribute) a;
                        if (ca.unit != null && ca.getTermSourceREF() != null) {
                            fireErrorItemEvent(new ErrorItemImpl("Sample "+s.getNodeName()+" has an attribute with both unit and term source", -1, getClass().getName()));
                        }
                    }
                }
                if (!hasOrganism) {
                    fireErrorItemEvent(new ErrorItemImpl("Sample "+s.getNodeName()+" has no Organism", -1, getClass().getName()));
                }
            }
        }
    }


    public static void main(String[] args) {
        new ValidationDriver().doMain(args);
    }
}
