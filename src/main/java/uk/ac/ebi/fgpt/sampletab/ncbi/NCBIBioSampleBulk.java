package uk.ac.ebi.fgpt.sampletab.ncbi;

import java.io.File;

import uk.ac.ebi.fgpt.sampletab.AbstractInfileDriver;

public class NCBIBioSampleBulk extends AbstractInfileDriver<NCBIBiosampleRunnable> {

    @Override
    protected NCBIBiosampleRunnable getNewTask(File inputFile) {
        inputFile = inputFile.getAbsoluteFile();
        File inputDir = inputFile.getParentFile();
        File outputFile = new File(inputDir, "sampletab.pre.txt");
        return new NCBIBiosampleRunnable(inputFile, outputFile);
    }

    public static void main(String[] args){
        new NCBIBioSampleBulk().doMain(args);
    }
    
}
