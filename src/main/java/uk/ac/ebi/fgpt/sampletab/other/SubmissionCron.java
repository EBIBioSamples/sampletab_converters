package uk.ac.ebi.fgpt.sampletab.other;

import java.io.File;

import org.kohsuke.args4j.Option;

import uk.ac.ebi.fgpt.sampletab.AbstractInfileDriver;

public class SubmissionCron extends AbstractInfileDriver<SubmissionCallable> {


    @Option(name = "--no-conan", usage = "do not trigger conan loads?")
    private boolean noconan = false;
    
    
    public static void main(String[] args) {
        new SubmissionCron().doMain(args);
    }

    @Override
    protected SubmissionCallable getNewTask(File inputFile) {
        return new SubmissionCallable(inputFile, !noconan);
    }
}
