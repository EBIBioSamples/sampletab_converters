package uk.ac.ebi.fgpt.sampletab.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.DatabaseAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.fg.myequivalents.managers.impl.base.BaseEntityMappingManager;
import uk.ac.ebi.fg.myequivalents.managers.interfaces.EntityMappingManager;
import uk.ac.ebi.fgpt.sampletab.AbstractInfileDriver;

public class MyEquivalentsLoader extends AbstractInfileDriver<uk.ac.ebi.fgpt.sampletab.tools.MyEquivalentsLoader.MyEquivalentsLoaderTask> {

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
        private File inFile;
        private EntityMappingManager emMgr = new BaseEntityMappingManager();
        
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

            for (SampleNode node : sampledata.scd.getNodes(SampleNode.class)) {
                List<String> bundle = new ArrayList<String>();
                bundle.add("biosamples-service:"+node.getSampleAccession());
                
                for (SCDNodeAttribute attr : node.getAttributes()) {
                    boolean isDatabase;
                    synchronized(DatabaseAttribute.class){
                        isDatabase = DatabaseAttribute.class.isInstance(attr);
                    }
                    if (isDatabase){
                        DatabaseAttribute dbattr = (DatabaseAttribute) attr;
        
                        String servicename = null;
                        if (dbattr.getAttributeValue().equals("ENA SRA")){
                            servicename = "ena-service";
                        }
                        if (servicename != null){
                            bundle.add(servicename+":"+dbattr.databaseID);
                        }
                    }
                }
                //convert the list into an array
                String[] bundlearray = new String[bundle.size()];
                for (int i = 0; i < bundle.size(); i++){
                    bundlearray[i] = bundle.get(i);
                }
                emMgr.storeMappingBundle( bundlearray );
            }
        }
    }
}
