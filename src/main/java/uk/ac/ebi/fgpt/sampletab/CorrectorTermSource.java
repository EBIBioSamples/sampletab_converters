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
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;

public class CorrectorTermSource {
    // logging
    private Logger log = LoggerFactory.getLogger(getClass());

    public void correct(SampleData sampledata) {
        //remove any unused term sources
        //first find which are used
        Set<String> usedTsNames = new HashSet<String>();
        for (SCDNode scdnode : sampledata.scd.getAllNodes()){
            for (SCDNodeAttribute attr : scdnode.getAttributes()){
                if (AbstractNodeAttributeOntology.class.isInstance(attr)){
                    AbstractNodeAttributeOntology attrOnt = (AbstractNodeAttributeOntology) attr;
                    //if this attribute has a term source at all
                    if (attrOnt.getTermSourceREF() != null && attrOnt.getTermSourceREF().trim().length() > 0){
                        //add it to the pool
                        usedTsNames.add(attrOnt.getTermSourceREF().trim());
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
        
        //in some cases, term sources have a null URL
        //for a sub-set of these we can guess them from being popular, e.g. EFO, NCBI Taxonomy, etc
        for (int i = 0; i < sampledata.msi.termSources.size(); i++){
            TermSource ts = sampledata.msi.termSources.get(i);
            if (ts.getURI() == null){
                if (ts.getName().equals("EFO")){
                    ts = new TermSource(ts.getName(), "http://www.ebi.ac.uk/efo/", null);
                    sampledata.msi.termSources.set(i, ts);
                }
            }
        }
    }
}
