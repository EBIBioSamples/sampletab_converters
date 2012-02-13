package uk.ac.ebi.fgpt.sampletab.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SCDNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabParser;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;

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

		SampleTabParser<SampleData> parser = new SampleTabParser<SampleData>();
		SampleData sampledata;
		try {
			sampledata = parser.parse(inFile);
		} catch (ParseException e) {
			log.error("Unable to parse "+inFile);
			e.printStackTrace();
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
            // TODO check for extra arguments?
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            help = true;
        }

        if (help) {
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
            return;
        }

        List<File> inputFiles = new ArrayList<File>();
        inputFiles = FileUtils.getMatchesGlob(inputFilename);
        Collections.sort(inputFiles);
        
        for (File inputFile : inputFiles){
        	process(inputFile);
        }
    }
}
