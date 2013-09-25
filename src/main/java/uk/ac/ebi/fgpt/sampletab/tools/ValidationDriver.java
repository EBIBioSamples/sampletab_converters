package uk.ac.ebi.fgpt.sampletab.tools;

import java.io.File;
import java.util.concurrent.Callable;

import org.mged.magetab.error.ErrorItem;
import org.mged.magetab.error.ErrorItemImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.datamodel.graph.Node;
import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.exception.ValidateException;
import uk.ac.ebi.arrayexpress2.magetab.listener.ErrorItemListener;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Organization;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Person;
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
    protected Callable<Void> getNewTask(File inputFile) {
        return new ValidationRunnable(inputFile);
    }
    
    private class ValidationRunnable implements Callable<Void> {

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
        public Void call() throws Exception {
            SampleData sampledata = null;
            try {
                sampledata = parser.parse(inputFile);
            } catch (ParseException e) {
                log.error("Error when parsing "+inputFile, e);
                throw e;
            }
            return null;
        }
        
    }
    
    
    private class ImprovedSampleTabValidator  extends SampleTabValidator {

        private boolean hasOrganism (SampleNode s ) {
            
            for (SCDNodeAttribute a : s.getAttributes()) {
                boolean isOrganism = false;
                synchronized(OrganismAttribute.class) {
                    isOrganism = OrganismAttribute.class.isInstance(a);
                }
                if (isOrganism) {
                    return true;
                }
            }
            
            for (Node p : s.getParentNodes()){
                boolean isSample = false;
                synchronized(SampleNode.class) {
                    isSample = SampleNode.class.isInstance(p);
                }
                if (isSample) {
                    SampleNode ps = (SampleNode) p;
                    if (hasOrganism(ps)){
                        return true;
                    }
                }
            }
            
            return false;
        }
                
                
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
            
            //validate pub med ids
            for (Publication p : sampledata.msi.publications) {
                Integer i = null;
                if (p.getPubMedID() != null && p.getPubMedID().length() > 0) {
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
            }
            
            //various field length validations for relational database
            for (Person p : sampledata.msi.persons) {
                if (p.getEmail() != null && p.getEmail().length() > 60) {
                    fireErrorItemEvent(new ErrorItemImpl("Too long email address "+p.getEmail(), -1, getClass().getName()));
                }
                if (p.getFirstName() != null && p.getFirstName().length() > 60) {
                    fireErrorItemEvent(new ErrorItemImpl("Too long first name "+p.getFirstName(), -1, getClass().getName()));
                }
                if (p.getInitials() != null && p.getInitials().length() > 30) {
                    fireErrorItemEvent(new ErrorItemImpl("Too long initials "+p.getInitials(), -1, getClass().getName()));
                }
                if (p.getLastName() != null && p.getLastName().length() > 60) {
                    fireErrorItemEvent(new ErrorItemImpl("Too long last name "+p.getLastName(), -1, getClass().getName()));
                }
            }
            for (Organization o : sampledata.msi.organizations) {
                if (o.getName() != null && o.getName().length() > 200) {
                    fireErrorItemEvent(new ErrorItemImpl("Too long organization name "+o.getEmail(), -1, getClass().getName()));
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

                boolean hasLatitude = false;
                boolean hasLongitude = false;
                
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
                        OrganismAttribute oa = (OrganismAttribute) a;
                        if (oa.getTermSourceID() == null || oa.getTermSourceID().length() == 0) {
                            fireErrorItemEvent(new ErrorItemImpl("Sample "+s.getNodeName()+" has no Organism Term Source REF for "+oa.getAttributeValue(), -1, getClass().getName()));
                        }
                    }
                    
                    //check that comments and characteristics don't have both units and term source refs
                    //check for latitude & longitude
                    if (isCommentAttribute) {
                        CommentAttribute ca = (CommentAttribute) a;
                        if (ca.unit != null && ca.getTermSourceREF() != null) {
                            fireErrorItemEvent(new ErrorItemImpl("Sample "+s.getNodeName()+" has an attribute with both unit and term source", -1, getClass().getName()));
                        }
                        if (ca.type.equalsIgnoreCase("latitude")) {
                            hasLatitude = true;
                        }
                        if (ca.type.equalsIgnoreCase("longitude")) {
                            hasLongitude = true;
                        }
                    }
                    if (isCharacteristicAttribute) {
                        CharacteristicAttribute ca = (CharacteristicAttribute) a;
                        if (ca.unit != null && ca.getTermSourceREF() != null) {
                            fireErrorItemEvent(new ErrorItemImpl("Sample "+s.getNodeName()+" has an attribute with both unit and term source", -1, getClass().getName()));
                        }
                        if (ca.type.equalsIgnoreCase("latitude")) {
                            hasLatitude = true;
                        }
                        if (ca.type.equalsIgnoreCase("longitude")) {
                            hasLongitude = true;
                        }
                    }
                }
                
                //check for organism attribute
                //also check nodes upstream for inheritance
                if (!hasOrganism(s)) {
                    fireErrorItemEvent(new ErrorItemImpl("Sample "+s.getNodeName()+" has no Organism", -1, getClass().getName()));
                }
                
                if (hasLatitude != hasLongitude) {
                    fireErrorItemEvent(new ErrorItemImpl("Sample "+s.getNodeName()+" has unblanaced lat/long", -1, getClass().getName()));   
                }
            }
        }
    }


    public static void main(String[] args) {
        new ValidationDriver().doMain(args);
    }
}
