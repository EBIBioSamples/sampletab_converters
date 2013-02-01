package uk.ac.ebi.fgpt.sampletab.guixml;

import java.io.File;
import java.util.Comparator;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.comparator.ComparatorSampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;


@SuppressWarnings("restriction")
public class SampleTabGuiComparison {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Argument(required=true, index=0, metaVar="PRE", usage = "before gui")
    private String preFilename;

    @Argument(required=true, index=1, metaVar="POST", usage = "after gui")
    private String postFilename;

    private Logger log = LoggerFactory.getLogger(getClass());
    

    private Comparator<SampleData> c = new ComparatorSampleData();
    
    public static void main(String[] args) {
        new SampleTabGuiComparison().doMain(args);
    }    
    
    public void doMain(String[] args) {

        CmdLineParser cmdParser = new CmdLineParser(this);
        try {
            // parse the arguments.
            cmdParser.parseArgument(args);
            // TODO check for extra arguments?
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            help = true;
        }

        if (help) {
            // print the list of available options
            cmdParser.printSingleLineUsage(System.err);
            System.err.println();
            cmdParser.printUsage(System.err);
            System.err.println();
            System.exit(1);
            return;
        }


        SampleTabSaferParser stParser = new SampleTabSaferParser();
        
        File preFile = new File(preFilename);
        File postFile = new File(postFilename);
        
        SampleData preSD = null;
        try {
            preSD = stParser.parse(preFile);
        } catch (ParseException e) {
            log.error("Unable to parse file "+preFile, e);
            return;
        }

        SampleData postSD = null;
        try {
            postSD = stParser.parse(postFile);
        } catch (ParseException e) {
            log.error("Unable to parse file "+postFile, e);
            return;
        }
        
        //do the comparison
        int score = c.compare(preSD, postSD);
        
        if (score == 0){
            log.info("They are the same");
        } else {
            log.info("They are different");
        }   
    }
}
