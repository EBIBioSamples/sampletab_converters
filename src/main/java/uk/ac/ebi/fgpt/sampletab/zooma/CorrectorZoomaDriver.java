package uk.ac.ebi.fgpt.sampletab.zooma;

import java.io.File;

import uk.ac.ebi.fgpt.sampletab.AbstractInfileDriver;

public class CorrectorZoomaDriver extends AbstractInfileDriver<CorrectorZoomaRunnable> {
    

    public static void main(String[] args) {
        new CorrectorZoomaDriver().doMain(args);
    }
    
    @Override
    protected CorrectorZoomaRunnable getNewTask(File inputFile) {
        return new CorrectorZoomaRunnable(inputFile);
    }
}
