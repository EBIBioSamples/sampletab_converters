package uk.ac.ebi.fgpt.sampletab;


import org.mged.magetab.error.ErrorItemImpl;

import uk.ac.ebi.arrayexpress2.magetab.exception.ValidateException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Organization;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Person;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.validator.SampleTabValidator;

public class LoadValidator extends SampleTabValidator {
    
    public synchronized void validate(SampleData sampledata) throws ValidateException {
        
        super.validate(sampledata);
        
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
            for (SCDNodeAttribute a : s.getAttributes()) {
                if (a.getAttributeType().length() > 200) {
                    fireErrorItemEvent(new ErrorItemImpl("Too long attribute name "+a.getAttributeType()+" on sample "+s.getNodeName(), -1, getClass().getName()));
                }
            }
        }
    }
}
