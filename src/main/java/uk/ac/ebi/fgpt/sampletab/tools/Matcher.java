package uk.ac.ebi.fgpt.sampletab.tools;

import java.io.File;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SCDNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.fgpt.sampletab.utils.FileGlobIterable;

public class Matcher {

    @Option(name = "-h", usage = "display help")
    private boolean help;

    @Option(name = "-i", aliases={"--input"}, usage = "input filename or glob")
    private String inputFilename;

    @Option(name = "-s", aliases={"--search"}, usage = "regex search string")
    private String search;

	// logging
	private Logger log = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) {
        new Matcher().doMain(args);
    }
	
	private void process(File inFile){

	    SampleTabSaferParser parser = new SampleTabSaferParser();
		SampleData sampledata;
		try {
			sampledata = parser.parse(inFile);
		} catch (ParseException e) {
			log.error("Unable to parse "+inFile, e);
			return;
		}
				

		for (SCDNode node : sampledata.scd.getAllNodes()) {
			for (SCDNodeAttribute attribute : node.getAttributes()) {
				if (attribute.getAttributeType().matches(search)){
					System.out.println(inFile+"\t"+node.getNodeName()+"\t"+attribute.getAttributeType()+"\t"+attribute.getAttributeValue());
				}
			}
		}
	}

    public void doMain(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            // parse the arguments.
            parser.parseArgument(args);
            //TODO check for extra arguments?
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
        
        for (File inputFile : new FileGlobIterable(inputFilename)){
        	process(inputFile);
        }
    }
}
