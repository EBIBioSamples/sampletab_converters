package uk.ac.ebi.fgpt.sampletab;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDriver {
    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    protected boolean help;

    private Logger log = LoggerFactory.getLogger(getClass());
    
    protected void doMain(String[] args){
        CmdLineParser cmdParser = new CmdLineParser(this);
        try {
            // parse the arguments.
            cmdParser.parseArgument(args);
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
    }

}
