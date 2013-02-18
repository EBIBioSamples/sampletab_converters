package uk.ac.ebi.fgpt.sampletab.tools;

import java.io.File;
import java.util.ArrayList;
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
import uk.ac.ebi.fg.myequivalents.resources.Resources;
import uk.ac.ebi.fgpt.sampletab.AbstractInfileDriver;

public class MyEquivalentsLoader extends AbstractInfileDriver<uk.ac.ebi.fgpt.sampletab.tools.MyEquivalentsLoader.MyEquivalentsLoaderTask> {

    
    private static String BIOSAMPLESSAMPLESSERVICE = "ebi.biosamples.samples";
    private static String BIOSAMPLESGROUPSSERVICE = "ebi.biosamples.groups";
    private static String ENASAMPLESSERVICE = "ebi.ena.samples";
    private static String ENAGROUPSSERVICE = "ebi.ena.groups";
    private static String ARRAYEXPRESSGROUPSERVICE = "ebi.arrayexpress.groups";
    private static String PRIDESAMPLESERVICE = "ebi.pride.samples";

    private final EntityMappingManager emMgr = Resources.getInstance().getMyEqManagerFactory().newEntityMappingManager();
    
    // logging
    private Logger log = LoggerFactory.getLogger(getClass());


    public static void main(String[] args) {
        new MyEquivalentsLoader().doMain(args);
    }
    
    @Override
    protected MyEquivalentsLoaderTask getNewTask(File inputFile) {
        return new MyEquivalentsLoaderTask(inputFile);
    }

    public class MyEquivalentsLoaderTask implements Runnable {
        private final File inFile;
        
        MyEquivalentsLoaderTask(File inFile){
            this.inFile = inFile;
            
        }
        
        public void run() {
            SampleTabSaferParser parser = new SampleTabSaferParser();
            SampleData sampledata;
            try {
                sampledata = parser.parse(this.inFile);
            } catch (ParseException e) {
                log.error("Unable to parse "+inFile, e);
                return;
            }

            //store group mappings
            List<String> bundle = new ArrayList<String>();
            for (GroupNode node : sampledata.scd.getNodes(GroupNode.class)) {
                
                bundle.add(BIOSAMPLESGROUPSSERVICE+":"+node.getGroupAccession());
                for (Database database : sampledata.msi.databases){
    
                    String servicename = null;
                    String serviceaccession = null;
                    if (database.getName().equals("ENA SRA")){
                        servicename = ENAGROUPSSERVICE;
                        serviceaccession = database.getID();
                    } else if (database.getName().equals("ArrayExpress")){
                        servicename = ARRAYEXPRESSGROUPSERVICE;
                        serviceaccession = database.getID();
                    }
                    
                    if (servicename != null && servicename.length() > 0 
                            && serviceaccession != null && serviceaccession.length() > 0){
                        bundle.add(servicename+":"+serviceaccession);
                    }
                }
                
                //convert the list into an array
                if (bundle.size() >= 2){
                    String[] bundlearray = new String[bundle.size()];
                    bundle.toArray(bundlearray);
                    
                    synchronized(emMgr){
                        emMgr.storeMappingBundle( bundlearray );
                    }
                }
            }

            //store sample mappings
            for (SampleNode node : sampledata.scd.getNodes(SampleNode.class)) {
                bundle.clear();
                bundle.add(BIOSAMPLESSAMPLESSERVICE+":"+node.getSampleAccession());
                
                for (SCDNodeAttribute attr : node.getAttributes()) {
                    boolean isDatabase;
                    synchronized(DatabaseAttribute.class){
                        isDatabase = DatabaseAttribute.class.isInstance(attr);
                    }
                    boolean isSameAs;
                    synchronized(SameAsAttribute.class){
                        isSameAs = SameAsAttribute.class.isInstance(attr);
                    }
                    if (isDatabase){
                        DatabaseAttribute dbattr = (DatabaseAttribute) attr;
        
                        String servicename = null;
                        String serviceaccession = null;
                        if (dbattr.getAttributeValue().equals("ENA SRA")
                                || dbattr.getAttributeValue().startsWith("EMBL-bank")){
                            servicename = ENASAMPLESSERVICE;
                            serviceaccession = dbattr.databaseID;
                        } else if (dbattr.getAttributeValue().equals("PRIDE")){
                            servicename = PRIDESAMPLESERVICE;
                            serviceaccession = dbattr.databaseID;
                        }
                        
                        if (servicename != null){
                            bundle.add(servicename+":"+serviceaccession);
                        }
                    } else if (isSameAs){
                        if (attr.getAttributeValue().matches("SAME[EN]A?[1-9][0-9]+")){
                            bundle.add(BIOSAMPLESSAMPLESSERVICE+":"+attr.getAttributeValue());
                        }
                    }
                }
                
                //convert the list into an array
                if (bundle.size() >= 2){
                    String[] bundlearray = new String[bundle.size()];
                    bundle.toArray(bundlearray);

                    synchronized(emMgr){
                        emMgr.storeMappingBundle( bundlearray );
                    }
                }
            }
        }
    }
}
