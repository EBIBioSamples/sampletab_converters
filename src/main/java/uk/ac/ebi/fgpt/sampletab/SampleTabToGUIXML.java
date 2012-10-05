package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.datamodel.graph.Node;
import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Database;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Organization;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Person;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Publication;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.GroupNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.AbstractNodeAttributeOntology;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

@SuppressWarnings("restriction")
public class SampleTabToGUIXML {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Argument(required=true, index=0, metaVar="OUTPUT", usage = "output filename")
    private String outputFilename;

    @Argument(required=true, index=1, metaVar="INPUT", usage = "input filename or globs")
    private List<String> inputFilenames;

    private Logger log = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) {
        new SampleTabToGUIXML().doMain(args);
    }

    private void writeAttribute(XMLStreamWriter xmlWriter, String cls, String classDefined, String dataType, String value) throws XMLStreamException{
        xmlWriter.writeStartElement("attribute");
        xmlWriter.writeAttribute("class", cls);
        xmlWriter.writeAttribute("classDefined", classDefined);
        xmlWriter.writeAttribute("dataType", dataType);
        if (value != null){
            xmlWriter.writeStartElement("value");
            
            value = XMLUtils.stripNonValidXMLCharacters(value);
            xmlWriter.writeCharacters(value);
            xmlWriter.writeEndElement(); //value
        }
        xmlWriter.writeEndElement(); //attribute
    }

    private void writeAttribute(XMLStreamWriter xmlWriter, AbstractNodeAttributeOntology attr, SampleData st) throws XMLStreamException{
        String cls = attr.getAttributeType();
        String value = attr.getAttributeValue();
        xmlWriter.writeStartElement("attribute");
        xmlWriter.writeAttribute("class", cls);
        xmlWriter.writeAttribute("classDefined", "false");
        xmlWriter.writeAttribute("dataType", "STRING");
        if (value != null){
            xmlWriter.writeStartElement("value");
            if (attr.getTermSourceREF() != null && attr.getTermSourceREF().trim().length() > 0
                    && attr.getTermSourceID() != null && attr.getTermSourceID().trim().length() > 0){
            //ontology stuff
            /*
<attribute class="Organism" classDefined="false" dataType="STRING">
    <value>
        <attribute class="Term Source REF" classDefined="true" dataType="OBJECT">
            <value>
                <object id="NCBI Taxonomy" class="Term Source" classDefined="true">
                    <attribute class="Term Source Name" classDefined="true" dataType="STRING">
                        <value>NCBI Taxonomy</value>
                    </attribute>
                    <attribute class="Term Source URI" classDefined="true" dataType="URI">
                        <value>http://www.ncbi.nlm.nih.gov/taxonomy</value>
                    </attribute>
                </object>
            </value>
        </attribute>
        <attribute class="Term Source ID" classDefined="true" dataType="STRING">
            <value>9606</value>
        </attribute>
        Homo sapiens
    </value>
</attribute>
             */
                xmlWriter.writeStartElement("attribute");
                xmlWriter.writeAttribute("class", "Term Source REF");
                xmlWriter.writeAttribute("classDefined", "true");
                xmlWriter.writeAttribute("dataType", "OBJECT");
                xmlWriter.writeStartElement("value");
                
                xmlWriter.writeStartElement("object");
                xmlWriter.writeAttribute("id", attr.getTermSourceREF());
                xmlWriter.writeAttribute("class", "Term Source REF");
                xmlWriter.writeAttribute("classDefined", "true");

                TermSource ts = st.msi.getTermSource(attr.getTermSourceREF());
                writeAttribute(xmlWriter, "Term Source Name", "true", "STRING", ts.getName());
                writeAttribute(xmlWriter, "Term Source URI", "true", "URI", ts.getURI());
                //TODO Term Source Version?

                xmlWriter.writeEndElement(); //object
                xmlWriter.writeEndElement(); //value
                xmlWriter.writeEndElement(); //attribute
                writeAttribute(xmlWriter, "Term Source ID", "true", "STRING", attr.getTermSourceID());
            }
            
            value = XMLUtils.stripNonValidXMLCharacters(value);
            xmlWriter.writeCharacters(value);
            xmlWriter.writeEndElement(); //value
        }
        xmlWriter.writeEndElement(); //attribute
    }
    
    private void preprocessSampleData(SampleData sd) throws ParseException{
        //ensure all samples are in a group
        //if any are not, create the group and put them in
        

        // All samples must be in a group
        // so create a new group and add all non-grouped samples to it
        GroupNode othergroup = new GroupNode("Other Group");
        othergroup.setGroupAccession(sd.msi.submissionIdentifier);
        for (SampleNode sample : sd.scd.getNodes(SampleNode.class)) {
            // check there is not an existing group first...
            boolean inGroup = false;
            for (Node n : sample.getChildNodes()){
                if (GroupNode.class.isInstance(n)){
                    inGroup = true;
                }
            }
            if (!inGroup){
                log.debug("Adding sample " + sample.getNodeName() + " to group " + othergroup.getNodeName());
                othergroup.addSample(sample);
            }
        }
        //only add the new group if it has any samples
        if (othergroup.getParentNodes().size() > 0){
            sd.scd.addNode(othergroup);        
            log.debug("Added Other group node");
            // also need to accession the new node
        }
        
    }
    
    public void writeSampleData(XMLStreamWriter xmlWriter, SampleData sd) throws XMLStreamException{

        //NB. this assumes all samples are in a group. This is a condition of the .toload.txt files
        //but not of sampletab.txt files.
        
        for (GroupNode g : sd.scd.getNodes(GroupNode.class)){
            
            if (g.getGroupAccession() == null){
                log.warn("Group has null accession "+g.getNodeName()+" ("+sd.msi.submissionIdentifier+")");
                continue;
            }
            
            log.debug("Group "+g.getNodeName());
            
            xmlWriter.writeStartElement("SampleGroup");
            xmlWriter.writeAttribute("id", g.getGroupAccession());

            writeAttribute(xmlWriter, "Submission Title", "true", "STRING", sd.msi.submissionTitle);
            writeAttribute(xmlWriter, "Submission Identifier", "true", "STRING", sd.msi.submissionIdentifier);
            writeAttribute(xmlWriter, "Submission Description", "true", "STRING", sd.msi.submissionDescription);
            writeAttribute(xmlWriter, "Submission Version", "true", "STRING", sd.msi.submissionVersion);
            writeAttribute(xmlWriter, "Submission Reference Layer", "true", "BOOLEAN", sd.msi.submissionReferenceLayer.toString());
            writeAttribute(xmlWriter, "Submission Release Date", "true", "STRING", sd.msi.getSubmissionReleaseDateAsString());
            writeAttribute(xmlWriter, "Submission Modification Date", "true", "STRING", sd.msi.getSubmissionUpdateDateAsString());
            writeAttribute(xmlWriter, "Name", "true", "BOOLEAN", g.getNodeName());
            writeAttribute(xmlWriter, "Group Accession", "true", "BOOLEAN", g.getGroupAccession());
            //group description?
            
            //organizations   
            if (sd.msi.organizations.size() > 0){
                xmlWriter.writeStartElement("attribute");
                xmlWriter.writeAttribute("class", "Organizations");
                xmlWriter.writeAttribute("classDefined", "true");
                xmlWriter.writeAttribute("dataType", "OBJECT");
                xmlWriter.writeStartElement("value");                     
                for (Integer i = 0; i < sd.msi.organizations.size(); i++){
                    Organization o = sd.msi.organizations.get(i);
                    if (o != null){
                        xmlWriter.writeStartElement("object");
                        xmlWriter.writeAttribute("id", "Org"+i.toString());
                        xmlWriter.writeAttribute("class", "Organization");
                        xmlWriter.writeAttribute("classDefined", "true");

                        writeAttribute(xmlWriter, "Organization Name", "true", "STRING", o.getName());
                        writeAttribute(xmlWriter, "Organization Address", "true", "STRING", o.getAddress());
                        writeAttribute(xmlWriter, "Organization URI", "true", "STRING", o.getURI());
                        writeAttribute(xmlWriter, "Organization Email", "true", "STRING", o.getEmail());
                        writeAttribute(xmlWriter, "Organization Role", "true", "STRING", o.getRole());
                        
                        xmlWriter.writeEndElement(); //object
                    }
                }
                xmlWriter.writeEndElement(); //value
                xmlWriter.writeEndElement(); //organizations
            }
            
            //persons
            if (sd.msi.persons.size() > 0){
                xmlWriter.writeStartElement("attribute");
                xmlWriter.writeAttribute("class", "Persons");
                xmlWriter.writeAttribute("classDefined", "true");
                xmlWriter.writeAttribute("dataType", "OBJECT");
                xmlWriter.writeStartElement("value");                     
                for (Integer i = 0; i < sd.msi.persons.size(); i++){
                    Person p = sd.msi.persons.get(i);
                    if (p != null){
                        xmlWriter.writeStartElement("object");
                        xmlWriter.writeAttribute("id", "Per"+i.toString());
                        xmlWriter.writeAttribute("class", "Person");
                        xmlWriter.writeAttribute("classDefined", "true");

                        writeAttribute(xmlWriter, "Person Last Name", "true", "STRING", p.getLastName());
                        writeAttribute(xmlWriter, "Person Initials", "true", "STRING", p.getInitials());
                        writeAttribute(xmlWriter, "Person First Name", "true", "STRING", p.getFirstName());
                        writeAttribute(xmlWriter, "Person Email", "true", "STRING", p.getEmail());
                        writeAttribute(xmlWriter, "Person Role", "true", "STRING", p.getRole());
                        
                        xmlWriter.writeEndElement(); //object
                    }
                }
                xmlWriter.writeEndElement(); //value
                xmlWriter.writeEndElement(); //persons
            }
            
            //publications
            if (sd.msi.publications.size() > 0){
                xmlWriter.writeStartElement("attribute");
                xmlWriter.writeAttribute("class", "Publications");
                xmlWriter.writeAttribute("classDefined", "true");
                xmlWriter.writeAttribute("dataType", "OBJECT");
                xmlWriter.writeStartElement("value");                     
                for (Integer i = 0; i < sd.msi.publications.size(); i++){
                    Publication p = sd.msi.publications.get(i);
                    if (p != null){
                        xmlWriter.writeStartElement("object");
                        xmlWriter.writeAttribute("id", "Pub"+i.toString());
                        xmlWriter.writeAttribute("class", "Publication");
                        xmlWriter.writeAttribute("classDefined", "true");

                        writeAttribute(xmlWriter, "Publication PubMed ID", "true", "STRING", p.getPubMedID());
                        writeAttribute(xmlWriter, "Publication DOI", "true", "STRING", p.getDOI());
                        
                        xmlWriter.writeEndElement(); //object
                    }
                }
                xmlWriter.writeEndElement(); //value
                xmlWriter.writeEndElement(); //publications
            }
            
            //term sources
            if (sd.msi.termSources.size() > 0){
                xmlWriter.writeStartElement("attribute");
                xmlWriter.writeAttribute("class", "Term Sources");
                xmlWriter.writeAttribute("classDefined", "true");
                xmlWriter.writeAttribute("dataType", "OBJECT");
                xmlWriter.writeStartElement("value");                     
                for (Integer i = 0; i < sd.msi.termSources.size(); i++){
                    TermSource t = sd.msi.termSources.get(i);
                    if (t != null && t.getName() != null){
                        xmlWriter.writeStartElement("object");
                        xmlWriter.writeAttribute("id", t.getName());
                        xmlWriter.writeAttribute("class", "Term Source");
                        xmlWriter.writeAttribute("classDefined", "true");

                        writeAttribute(xmlWriter, "Term Source Name", "true", "STRING", t.getName());
                        writeAttribute(xmlWriter, "Term Source URI", "true", "STRING", t.getURI());
                        writeAttribute(xmlWriter, "Term Source Version", "true", "STRING", t.getVersion());
                        
                        xmlWriter.writeEndElement(); //object
                    }
                }
                xmlWriter.writeEndElement(); //value
                xmlWriter.writeEndElement(); //term sources
            }
            
            //database entries
            if (sd.msi.databases.size() > 0){
                xmlWriter.writeStartElement("attribute");
                xmlWriter.writeAttribute("class", "Databases");
                xmlWriter.writeAttribute("classDefined", "true");
                xmlWriter.writeAttribute("dataType", "OBJECT");
                xmlWriter.writeStartElement("value");                     
                for (Integer i = 0; i < sd.msi.databases.size(); i++){
                    Database d = sd.msi.databases.get(i);
                    if (d != null){
                        xmlWriter.writeStartElement("object");
                        xmlWriter.writeAttribute("id", "Dat"+i.toString());
                        xmlWriter.writeAttribute("class", "Database");
                        xmlWriter.writeAttribute("classDefined", "true");

                        writeAttribute(xmlWriter, "Database Name", "true", "STRING", d.getName());
                        writeAttribute(xmlWriter, "Database ID", "true", "STRING", d.getID());
                        writeAttribute(xmlWriter, "Database URI", "true", "STRING", d.getURI());
                        
                        xmlWriter.writeEndElement(); //object
                    }
                }
                xmlWriter.writeEndElement(); //value
                xmlWriter.writeEndElement(); //databaases
            }
            
            Set<String> attributeTypes = new HashSet<String>();
            
            for (Node s : g.getParentNodes()) {
                log.debug("Node "+s.getNodeName());
                //these should all be samples, but have to check anyway...
                if (SampleNode.class.isInstance(s)) {
                    SampleNode sample = (SampleNode) s;
                    xmlWriter.writeStartElement("Sample");
                    xmlWriter.writeAttribute("groupId", g.getGroupAccession());
                    xmlWriter.writeAttribute("id", sample.getSampleAccession());

                    writeAttribute(xmlWriter, "Name", "false", "STRING", sample.getNodeName());
                    attributeTypes.add("Name");
                    
                    writeAttribute(xmlWriter, "Sample Accession", "false", "STRING", sample.getSampleAccession());
                    attributeTypes.add("Sample Accession");
                    
                    writeAttribute(xmlWriter, "Sample Description", "false", "STRING", sample.getSampleDescription());
                    attributeTypes.add("Sample Description");
                    
                    for (SCDNodeAttribute a : sample.getAttributes()){
                        if (a.getAttributeValue() != null && a.getAttributeValue().length()>0){
                            synchronized(AbstractNodeAttributeOntology.class){
                                if (AbstractNodeAttributeOntology.class.isInstance(a)){
                                    writeAttribute(xmlWriter, (AbstractNodeAttributeOntology) a, sd);
                                } else {
                                    writeAttribute(xmlWriter, a.getAttributeType(), "false", "STRING", a.getAttributeValue());
                                }
                            }
                            attributeTypes.add(a.getAttributeType());
                            log.debug("Attribute "+a.getAttributeType()+" "+a.getAttributeValue());
                        }
                    }
                    
                    //implicit derived from
                    for (Node p : s.getParentNodes()) { 
                        //these should all be samples, but have to check anyway...
                        if (SampleNode.class.isInstance(p)) {
                            SampleNode parent = (SampleNode) p;
                            writeAttribute(xmlWriter, "Derived From", "false", "STRING", parent.getSampleAccession());
                            attributeTypes.add("Derived From");
                        }
                    }
                    
                    xmlWriter.writeEndElement(); //Sample
                }
            }
            
            //write out precomputed attribute summary
            xmlWriter.writeCharacters("\n");
            xmlWriter.writeStartElement("SampleAttributes");
            for (String attributeType : attributeTypes){
                writeAttribute(xmlWriter, attributeType, "false", "STRING", null);
            }
            xmlWriter.writeEndElement(); //SampleAttributes
            
            xmlWriter.writeEndElement(); //SampleGroup
        }
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

        log.info("Looking for input files");
        List<File> inputFiles = new ArrayList<File>();
        for (String inputFilename : inputFilenames){
            inputFiles.addAll(FileUtils.getMatchesGlob(inputFilename));
        }
        log.info("Found " + inputFiles.size() + " input files");
        //TODO no duplicates
        Collections.sort(inputFiles);

        XMLOutputFactory output = XMLOutputFactory.newInstance();
        
        
        FileWriter outputWriter = null;
        try {
            //outputWriter = new FileWriter(outputFilename);
            //XMLStreamWriter xmlWriter = output.createXMLStreamWriter(outputWriter);
            
            Processor processor = new Processor(false);
            Serializer serializer = processor.newSerializer(new File(outputFilename));
            serializer.setOutputProperty(Property.INDENT, "yes");
            XMLStreamWriter xmlWriter = serializer.getXMLStreamWriter();
            
            xmlWriter.writeStartDocument();
            xmlWriter.writeStartElement("Biosamples");
            
            for (File inputFile : inputFiles){
                log.info("File "+inputFile);
                SampleData sd = null;
                SampleTabSaferParser stParser = new SampleTabSaferParser();
                try {
                    sd = stParser.parse(inputFile);
                } catch (ParseException e) {
                    log.error("Unable to parse file "+inputFile, e);
                }
                
                if (sd != null){
                
                    //if release date is in the future, dont output
                    if (sd.msi.submissionReleaseDate == null){
                        log.info("No release date, skipping");
                        continue;
                    } else if (sd.msi.submissionReleaseDate.after(new Date())){
                        log.info("Future release, skipping");
                        continue;
                    }
                    
                    try {
                        preprocessSampleData(sd);
                    } catch (ParseException e) {
                        log.error("Unable to preprocess "+inputFile, e);
                        continue;
                    }
                    writeSampleData(xmlWriter, sd);
                }
            }
            xmlWriter.writeEndDocument();
            
            xmlWriter.close();
        } catch (XMLStreamException e) {
            log.error("Problem parsing XML", e);
        } catch (SaxonApiException e) {
            log.error("Problem parsing XML", e);
        } finally {
            if (outputWriter != null){
                try{
                    outputWriter.close();
                } catch (IOException e) {
                    //do nothing
                }
            }
        }
        
    }
}
