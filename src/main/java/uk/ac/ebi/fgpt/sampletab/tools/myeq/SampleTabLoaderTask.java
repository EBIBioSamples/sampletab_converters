package uk.ac.ebi.fgpt.sampletab.tools.myeq;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Database;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.GroupNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.DatabaseAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SameAsAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.fg.myequivalents.managers.interfaces.EntityMappingManager;

public class SampleTabLoaderTask implements Runnable {
    
    private final File inFile;
    private final EntityMappingManager emMgr;

    private Logger log = LoggerFactory.getLogger(getClass());
    
    public SampleTabLoaderTask(File inFile, EntityMappingManager emMgr){
        this.inFile = inFile;
        this.emMgr = emMgr;
    }
    
    @Override
    public void run() {
        SampleTabSaferParser parser = new SampleTabSaferParser();
        SampleData sampledata;
        try {
            sampledata = parser.parse(this.inFile);
        } catch (ParseException e) {
            log.error("Unable to parse "+inFile+". message = e.getMessage()", e);
            return;
        }

        //store group mappings
        List<String> bundle = new ArrayList<String>();
        for (GroupNode node : sampledata.scd.getNodes(GroupNode.class)) {
            
            bundle.add(MyEqProperties.getBioSamplesGroupsService()+":"+node.getGroupAccession());
            for (Database database : sampledata.msi.databases) {

                String servicename = null;
                String serviceaccession = null;
                if (database.getName().equals("ENA SRA")) {
                    servicename = MyEqProperties.getENAGroupsService();
                    serviceaccession = database.getID();
                } else if (database.getName().equals("ArrayExpress")) {
                    servicename = MyEqProperties.getArrayExpressGroupsService();
                    serviceaccession = database.getID();
                } else {
                    log.warn("Unrecognized database "+database.getName());
                }
                
                if (servicename != null && servicename.length() > 0 
                        && serviceaccession != null && serviceaccession.length() > 0) {
                    bundle.add(servicename+":"+serviceaccession);
                }
            }
            
            storeBundle(bundle);
        }

        //store sample mappings
        for (SampleNode node : sampledata.scd.getNodes(SampleNode.class)) {
            bundle.clear();
            bundle.add(MyEqProperties.getBioSamplesSamplesService()+":"+node.getSampleAccession());
            
            for (SCDNodeAttribute attr : node.getAttributes()) {
                boolean isDatabase;
                synchronized(DatabaseAttribute.class) {
                    isDatabase = DatabaseAttribute.class.isInstance(attr);
                }
                if (isDatabase) {
                    DatabaseAttribute dbattr = (DatabaseAttribute) attr;
                    if (dbattr.getAttributeValue().equals("ENA SRA")
                            || dbattr.getAttributeValue().startsWith("EMBL-bank")) {
                        bundle.add(MyEqProperties.getENASamplesService()+":"+dbattr.databaseID);
                    } else if (dbattr.getAttributeValue().equals("PRIDE")) {
                        bundle.add(MyEqProperties.getPrideSamplesService()+":"+dbattr.databaseID);
                    } else if (dbattr.getAttributeValue().equals("COSMIC")) {
                        bundle.add(MyEqProperties.getCosmicSamplesService()+":"+dbattr.databaseID);
                    } else {
                        log.warn("Unrecognized database "+dbattr.getAttributeValue());
                    }
                }
                
                boolean isSameAs;
                synchronized(SameAsAttribute.class) {
                    isSameAs = SameAsAttribute.class.isInstance(attr);
                }
                if (isSameAs) {
                    if (attr.getAttributeValue().matches("SAM[EN]A?[1-9][0-9]+")) {
                        bundle.add(MyEqProperties.getBioSamplesSamplesService()+":"+attr.getAttributeValue());
                    } else {
                        log.warn("Unrecognized SameAs "+attr.getAttributeValue());
                    }
                }
            }
            
            storeBundle(bundle);
        }
    }
    
    private void storeBundle(Collection<String> bundle){
        if (bundle.size() >= 2){
            //convert the list into an array
            String[] bundlearray = new String[bundle.size()];
            bundle.toArray(bundlearray);

            log.debug("start bundle");
            for (String thing : bundle){
                log.debug(thing);
            }
            log.debug("end bundle");
            
            synchronized(emMgr){
                emMgr.storeMappingBundle( bundlearray );
            }
        }
        
    }
}