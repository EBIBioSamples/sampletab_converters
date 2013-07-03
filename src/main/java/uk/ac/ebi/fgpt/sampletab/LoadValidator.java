package uk.ac.ebi.fgpt.sampletab;

import java.util.Date;

import org.mged.magetab.error.ErrorItemImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.datamodel.graph.Node;
import uk.ac.ebi.arrayexpress2.magetab.exception.ValidateException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Organization;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Person;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.GroupNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.validator.SampleTabValidator;

public class LoadValidator extends SampleTabValidator {

    private Logger log = LoggerFactory.getLogger(getClass());
    
    public synchronized void validate(SampleData sampledata) throws ValidateException {
        
        super.validate(sampledata);
        
        Date now = new Date();

        //release date is in the future
        if (sampledata.msi.submissionReleaseDate.after(now)){
            //errors.add(getErrorItemFromCode(1528));
            //dont throw this as an error, but warn instead
            log.warn("Release date is in the future "+sampledata.msi.getSubmissionReleaseDateAsString());
        }
        
        //check all samples are in at least one group
        for (SampleNode s : sampledata.scd.getNodes(SampleNode.class)) {
            if (s.getChildNodes().size() == 0) {
                fireErrorItemEvent(new ErrorItemImpl("Sample "+s.getNodeName()+" is not in any group", -1, getClass().getName()));
            } else if (s.getChildNodes().size() >= 2) {
                fireErrorItemEvent(new ErrorItemImpl("Sample "+s.getNodeName()+" has 2+ child nodes", -1, getClass().getName()));
            } else if (s.getChildNodes().size() == 1) {
                for (Node n : s.getChildNodes()) {
                    synchronized(GroupNode.class) {
                        if (!GroupNode.class.isInstance(n)) {
                            fireErrorItemEvent(new ErrorItemImpl("Sample "+s.getNodeName()+" has a child node that is not a group", -1, getClass().getName()));
                        }
                    }
                }
            } 
        }
        
        //must have zero or one MSI Database object (this is a bug to be fixed in GUI)
        if (sampledata.msi.databases.size() > 1) {
            fireErrorItemEvent(new ErrorItemImpl("Has multiple MSI Database obejcts", -1, getClass().getName()));
        }
        
        //must have SAMExxxx or SAMEAxxxx accessions (this is a bug to be fixed in GUI)
        for (SampleNode s : sampledata.scd.getNodes(SampleNode.class)) {
           if (s.getSampleAccession() == null || !s.getSampleAccession().matches("^SAMEA?[1-9][0-9]$")) {
               fireErrorItemEvent(new ErrorItemImpl("Sample "+s.getNodeName()+" has a non-EBI accession "+s.getSampleAccession(), -1, getClass().getName()));
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
    }
}
