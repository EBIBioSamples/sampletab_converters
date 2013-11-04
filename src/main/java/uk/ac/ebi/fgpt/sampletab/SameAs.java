package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SameAsAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.arrayexpress2.sampletab.validator.SampleTabValidator;
import uk.ac.ebi.fgpt.sampletab.utils.CachedParser;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;

public class SameAs {
    
    @Option(name = "--sampletab-path", usage = "SampleTab path")
    private String stPath = ".";
    
    @Option(name = "--output", usage = "output filename")
    private String outputFilename;

    @Argument
    private List<String> arguments = new ArrayList<String>();

    @Option(name = "-h", usage = "display help")
    private boolean help;
    
    // logging
    private final Logger log = LoggerFactory.getLogger(getClass());
            
    public SampleData convert(SampleData st) throws IOException {
        for (SampleNode s : st.scd.getNodes(SampleNode.class)) {
            //itterate over a copy so we can add attributes as we go
            for (SCDNodeAttribute a: new ArrayList<SCDNodeAttribute>(s.getAttributes())){
                //Do AE=AE equivalence
                if (a.getAttributeType().equals("comment[Source EXP]")){
                    
                    File sourceFile = SampleTabUtils.getSubmissionDirFile("GA"+a.getAttributeValue());
                    
                    SampleData sd = null;
                    log.debug("reading "+sourceFile);
                    try {
                        sd = CachedParser.get(sourceFile);
                    } catch (ParseException e) {
                        log.error("Unable to read "+sourceFile, e);
                        continue;
                    }
                    
                    if (sd != null) {
                        //try to get a sample with the same name as sample of interest
                        String sampleName = s.getNodeName();
                        sampleName = sampleName.replace(a.getAttributeValue()+":", "");
                        log.debug("looking for sample "+sampleName);
                        
                        SampleNode t = sd.scd.getNode(sampleName, SampleNode.class);
                        if (t != null) {
                            s.addAttribute(new SameAsAttribute(t.getSampleAccession()));
                        }
                    }
                }
                //Do AE=SRA equivalence
                if (a.getAttributeType().equals("comment[ENA_SAMPLE]")){
                    //TODO finish this after SRA=SRA is solved
                }
            }
        }
        return st;
    }

    public void convert(SampleData sampleIn, Writer writer) throws IOException {
        log.info("recieved sampletab, preparing to convert");
        SampleData sampleOut = convert(sampleIn);
        log.info("sampletab converted, preparing to output");
        SampleTabWriter sampletabwriter = new SampleTabWriter(writer);
        log.info("created SampleTabWriter");
        sampletabwriter.write(sampleOut);
        sampletabwriter.close();

    }

    public void convert(File sampletabFile, Writer writer) throws ParseException, IOException {
        log.info("preparing to load SampleData");
        SampleTabSaferParser stparser = new SampleTabSaferParser(new SampleTabValidator());;
        log.info("created SampleTabParser<SampleData>, beginning parse");
        SampleData st = stparser.parse(sampletabFile);
        convert(st, writer);
    }

    public void convert(File inputFile, String outputFilename) throws ParseException, IOException  {
        convert(inputFile, new File(outputFilename));
    }

    public void convert(File inputFile, File outputFile) throws ParseException, IOException {
        convert(inputFile, new FileWriter(outputFile));
    }

    public void convert(String inputFilename, Writer writer) throws ParseException, IOException {
        convert(new File(inputFilename), writer);
    }

    public void convert(String inputFilename, File outputFile) throws ParseException, IOException{
        convert(inputFilename, new FileWriter(outputFile));
    }

    public void convert(String inputFilename, String outputFilename) throws ParseException, IOException {
        convert(inputFilename, new File(outputFilename));
    }
}
