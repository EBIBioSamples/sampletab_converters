package uk.ac.ebi.fgpt.sampletab.zooma;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabParser;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.fgpt.sampletab.AbstractInfileDriver;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;

public class CorrectorZoomaDriver extends AbstractInfileDriver<CorrectorZoomaRunnable> {
    

    public static void main(String[] args) {
        new CorrectorZoomaDriver().doMain(args);
    }
    
    @Override
    protected CorrectorZoomaRunnable getNewTask(File inputFile) {
        return new CorrectorZoomaRunnable(inputFile);
    }
}
