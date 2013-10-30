package uk.ac.ebi.fgpt.sampletab.tools;

import java.io.File;

import uk.ac.ebi.fgpt.sampletab.AbstractInfileDriver;
import uk.ac.ebi.fgpt.sampletab.SampleTabToLoadDriver;

public class APITestDriver extends AbstractInfileDriver<APITestCallable> {

    protected APITestCallable getNewTask(File inputFile) {
        return new APITestCallable(inputFile);
    }

    public static void main(String[] args) {
        new APITestDriver().doMain(args);
    }
    
    
}
