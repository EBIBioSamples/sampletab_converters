package uk.ac.ebi.fgpt.sampletab;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public abstract class AbstractDriver {
    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    protected boolean help;

    protected CmdLineParser cmdParser = new CmdLineParser(this);
    
    protected void doMain(String[] args) {
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
