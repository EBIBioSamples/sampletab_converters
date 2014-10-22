package uk.ac.ebi.fgpt.sampletab;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SCDNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.AbstractNodeAttributeOntology;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;

public class CorrectorTermSource {
    // logging
    private Logger log = LoggerFactory.getLogger(getClass());

    public void correct(SampleData sampledata) {
        //some samples have a term source ref but no term source id
        //ideally, we should do a full lookup against that term source
        //for the moment the information will be dropped
        for (SCDNode scdnode : sampledata.scd.getAllNodes()){
            for (SCDNodeAttribute attr : scdnode.getAttributes()){
                if (AbstractNodeAttributeOntology.class.isInstance(attr)){
                    AbstractNodeAttributeOntology attrOnt = (AbstractNodeAttributeOntology) attr;
                    if (attrOnt.getTermSourceREF() != null && attrOnt.getTermSourceREF().trim().length() > 0){
                        if (attrOnt.getTermSourceID() == null || attrOnt.getTermSourceID().trim().length() == 0){
                            log.info("Removing un-IDed term source "+attrOnt.getTermSourceREF()+" : "+attrOnt.getAttributeValue());
                            attrOnt.setTermSourceREF(null);
                            attrOnt.setTermSourceID(null);
                        }
                    }
                }
            }
        }
        
        
        
        //remove any unused term sources
        //first find which are used
        Set<String> usedTsNames = new HashSet<String>();
        for (SCDNode scdnode : sampledata.scd.getAllNodes()){
            for (SCDNodeAttribute attr : scdnode.getAttributes()){
                //have to cast attributes to classes which contain ontology references
                if (AbstractNodeAttributeOntology.class.isInstance(attr)){
                    AbstractNodeAttributeOntology attrOnt = (AbstractNodeAttributeOntology) attr;
                    //if this attribute has a term source at all
                    if (attrOnt.getTermSourceREF() != null && attrOnt.getTermSourceREF().trim().length() > 0){
                        //add it to the pool
                        usedTsNames.add(attrOnt.getTermSourceREF().trim());
                    }
                }
                //check for ontologies used by units
                if (CommentAttribute.class.isInstance(attr)) {
                    CommentAttribute comm = (CommentAttribute) attr;
                    if (comm.unit != null) {

                        //if this attribute has a term source at all
                        if (comm.unit.getTermSourceREF() != null 
                                && comm.unit.getTermSourceREF().trim().length() > 0) {
                            //add it to the pool
                            usedTsNames.add(comm.unit.getTermSourceREF().trim());
                            log.info("Found unit Term Source REF "+comm.unit.getTermSourceREF());
                        }
                    }
                    
                } else if (CharacteristicAttribute.class.isInstance(attr)) {
                    CharacteristicAttribute charatt = (CharacteristicAttribute) attr;
                    if (charatt.unit != null) {

                        //if this attribute has a term source at all
                        if (charatt.unit.getTermSourceREF() != null 
                                && charatt.unit.getTermSourceREF().trim().length() > 0) {
                            //add it to the pool
                            usedTsNames.add(charatt.unit.getTermSourceREF().trim());
                            log.info("Found unit Term Source REF "+charatt.unit.getTermSourceREF());
                        }
                    }
                    
                }
            }
        }
        
        //now trim them from the msi
        //use a new list so original can be deleted from
        for (TermSource ts: new ArrayList<TermSource>(sampledata.msi.termSources)){
            if (!usedTsNames.contains(ts.getName())) {
                log.info("Removing unused term source "+ts.getName());
                sampledata.msi.termSources.remove(ts);
            }
        }
        
        //add to the msi any term source which samples use
        for (String usedTsName : usedTsNames) {
            boolean exists = false;
            for (TermSource ts: new ArrayList<TermSource>(sampledata.msi.termSources)) {
                if (usedTsName.equals(ts.getName())) {
                    exists = true;
                    break;
                }
            }
            
            if (!exists) {
                TermSource ts = new TermSource(usedTsName, null, null);
                sampledata.msi.termSources.add(ts);
            }
        }
        
        /*
         * UPDATE: Actually, we want to leave these in so you can use an ID to refer to an URI without needing the source name
        //in some cases, term sources have a null URL
        //for a sub-set of these we can guess them from being popular, e.g. EFO, NCBI Taxonomy, etc
        //for others, delete them as they are not useful
        for (int i = 0; i < sampledata.msi.termSources.size(); i++){
            TermSource ts = sampledata.msi.termSources.get(i);
            log.debug("term source "+ts.getName()+" "+ts.getURI());
            if (ts.getURI() == null){
                log.debug("null URI term source "+ts.getName());
                TermSource tsnew = TermSourceUtils.guessURL(ts);
                if (tsnew == null) {
                    log.debug("Removing null term source "+ts.getName());
                    sampledata.msi.termSources.remove(i);
                    //because the list is now shorter, adjust the counter too
                    i--;
                    //now remove any reference to this ontology on any node
                    for (SCDNode scdnode : sampledata.scd.getAllNodes()){
                        for (SCDNodeAttribute attr : scdnode.getAttributes()){
                            if (AbstractNodeAttributeOntology.class.isInstance(attr)){
                                AbstractNodeAttributeOntology attrOnt = (AbstractNodeAttributeOntology) attr;
                                //if this attribute has this term source, remove it
                                if (attrOnt.getTermSourceREF() != null && attrOnt.getTermSourceREF().equals(ts.getName())){
                                    attrOnt.setTermSourceREF(null);
                                    attrOnt.setTermSourceIDInteger(null);
                                }
                            }
                        }
                    }
                } else {
                    sampledata.msi.termSources.set(i, tsnew);
                }
            }
        }*/
        //TODO add a date if no version present?
    }
}
