package uk.ac.ebi.fgpt.sampletab.tools.myeq;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fg.myequivalents.managers.interfaces.EntityMappingManager;
import uk.ac.ebi.fg.myequivalents.resources.Resources;
import uk.ac.ebi.fgpt.sampletab.AbstractInfileDriver;

public class SampleTabLoaderDriver extends AbstractInfileDriver<SampleTabLoaderTask> {

    
    private final EntityMappingManager emMgr = Resources.getInstance().getMyEqManagerFactory().newEntityMappingManager();
    
    // logging
    private Logger log = LoggerFactory.getLogger(getClass());


    public static void main(String[] args) {
        new SampleTabLoaderDriver().doMain(args);
    }
    
    @Override
    protected SampleTabLoaderTask getNewTask(File inputFile) {
        return new SampleTabLoaderTask(inputFile, emMgr);
    }

}
