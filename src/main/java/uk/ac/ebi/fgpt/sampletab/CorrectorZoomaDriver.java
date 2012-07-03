package uk.ac.ebi.fgpt.sampletab;

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
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;

public class CorrectorZoomaDriver {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Argument(required=true, index=0, metaVar="INPUT", usage = "input filenames or globs")
    private List<String> inputFilenames;
    
    private final CorrectorZooma zooma = new CorrectorZooma();
    private final SampleTabSaferParser stParser = new SampleTabSaferParser();
    
    private Logger log = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) {
        new CorrectorZoomaDriver().doMain(args);
    }

    public void doMain(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            // parse the arguments.
            parser.parseArgument(args);
            // TODO check for extra arguments?
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            help = true;
        }

        if (help) {
            // print the list of available options
            parser.printSingleLineUsage(System.err);
            System.err.println();
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
            return;
        }

               
        for (String inputFilename : inputFilenames){
            for (File inputFile : FileUtils.getMatchesGlob(inputFilename)){
                
                SampleData sd;
                try {
                    sd = stParser.parse(inputFile);
                    zooma.correct(sd);
                } catch (ParseException e) {
                    log.error("Unable ot parse "+inputFile);
                    e.printStackTrace();
                }
            }
        }
                
    }
}
