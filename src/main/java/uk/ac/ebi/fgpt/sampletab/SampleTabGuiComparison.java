package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.Serializer;
import net.sf.saxon.s9api.Serializer.Property;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.datamodel.graph.Node;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.graph.UnresolvedPlaceholderNode;
import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.comparator.ComparatorSampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Database;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Organization;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Person;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Publication;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.GroupNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.AbstractNodeAttributeOntology;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.ChildOfAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.DerivedFromAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.MaterialAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SameAsAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SexAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;


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
