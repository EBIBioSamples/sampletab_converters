package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
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
public class SampleTabFromGUIXML {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Argument(required=true, index=0, metaVar="OUTPUT", usage = "output filename")
    private String outputFilename;

    @Argument(required=true, index=1, metaVar="INPUT", usage = "input filename or globs")
    private List<String> inputFilenames;

    private Logger log = LoggerFactory.getLogger(getClass());
    
    public static SimpleDateFormat simpledateformat = new SimpleDateFormat("yyyy/MM/dd");

    public static void main(String[] args) {
        new SampleTabFromGUIXML().doMain(args);
    }

    private Organization parseOrganization(Element org){
        String name = null;
        String address = null;
        String uri = null;
        String email = null;
        String role = null;

        for (Element attribute : XMLUtils.getChildrenByName(org, "attribute")){
            Element value = XMLUtils.getChildByName(attribute, "value");
            String attrKey = attribute.attributeValue("class");
            if (value != null){
                if (attrKey.equals("Organization Name")){
                    name = value.getTextTrim();
                } else if (attrKey.equals("Organization Address")){
                    address = value.getTextTrim();
                } else if (attrKey.equals("Organization URI")){
                    uri = value.getTextTrim();
                } else if (attrKey.equals("Organization Email")){
                    email = value.getTextTrim();
                } else if (attrKey.equals("Organization Role")){
                    role = value.getTextTrim();
                } 
            }
        }
        
        Organization o = new Organization(name, address, uri, email, role);
        return o;
    }

    private Person parsePerson(Element org){
        String lastname = null;
        String firstname = null;
        String initials = null;
        String email = null;
        String role = null;

        for (Element attribute : XMLUtils.getChildrenByName(org, "attribute")){
            Element value = XMLUtils.getChildByName(attribute, "value");
            String attrKey = attribute.attributeValue("class");
            if (value != null){
                if (attrKey.equals("Person Last Name")){
                    lastname = value.getTextTrim();
                } else if (attrKey.equals("Person First Name")){
                    firstname = value.getTextTrim();
                } else if (attrKey.equals("Person Initials")){
                    initials = value.getTextTrim();
                } else if (attrKey.equals("Person Email")){
                    email = value.getTextTrim();
                } else if (attrKey.equals("Person Role")){
                    role = value.getTextTrim();
                } 
            }
        }
        
        Person o = new Person(lastname, initials, firstname, email, role);
        return o;
    }

    private Publication parsePublication(Element org){
        String pubmed = null;
        String doi = null;

        for (Element attribute : XMLUtils.getChildrenByName(org, "attribute")){
            Element value = XMLUtils.getChildByName(attribute, "value");
            String attrKey = attribute.attributeValue("class");
            if (value != null){
                if (attrKey.equals("Publication PubMed ID")){
                    pubmed = value.getTextTrim();
                } else if (attrKey.equals("Publication DOI")){
                    doi = value.getTextTrim();
                } 
            }
        }
        
        Publication o = new Publication(pubmed, doi);
        return o;
    }

    private TermSource parseTermSource(Element org){
        String name = null;
        String uri = null;
        String version = null;

        for (Element attribute : XMLUtils.getChildrenByName(org, "attribute")){
            Element value = XMLUtils.getChildByName(attribute, "value");
            String attrKey = attribute.attributeValue("class");
            if (value != null){
                if (attrKey.equals("Term Source Name")){
                    name = value.getTextTrim();
                } else if (attrKey.equals("Term Source URI")){
                    uri = value.getTextTrim();
                } else if (attrKey.equals("Term Source Version")){
                    version = value.getTextTrim();
                } 
            }
        }
        
        TermSource o = new TermSource(name, uri, version);
        return o;
    }

    private Database parseDatabase(Element org){
        String name = null;
        String uri = null;
        String id = null;

        for (Element attribute : XMLUtils.getChildrenByName(org, "attribute")){
            Element value = XMLUtils.getChildByName(attribute, "value");
            String attrKey = attribute.attributeValue("class");
            if (value != null){
                if (attrKey.equals("Database Name")){
                    name = value.getTextTrim();
                } else if (attrKey.equals("Database URI")){
                    uri = value.getTextTrim();
                } else if (attrKey.equals("Database ID")){
                    id = value.getTextTrim();
                } 
            }
        }
        
        Database o = new Database(name, uri, id);
        return o;
    }
    
    private void applyTermSource(Element value, AbstractNodeAttributeOntology a){
        for (Element attr : XMLUtils.getChildrenByName(value, "attribute")){
            Element value2 = XMLUtils.getChildByName(attr, "value");
            if (attr.attribute("class").equals("Term Source REF")){
                Element obj = XMLUtils.getChildByName(value2, "object");
                a.setTermSourceREF(obj.attributeValue("id"));
            } else if (attr.attributeValue("class").equals("Term Source ID")){
                a.setTermSourceID(value2.getTextTrim());
            }  
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

        SampleData st = new SampleData();
        
        for (File inputFile : inputFiles){
            try {
                Document xml = XMLUtils.getDocument(inputFile);
                
                Element root = xml.getRootElement();
                
                for (Element group : XMLUtils.getChildrenByName(root, "SampleGroup")){
                    GroupNode groupNode = new GroupNode();
                    groupNode.setGroupAccession(group.attributeValue("id"));
                    
                    for (Element attribute : XMLUtils.getChildrenByName(group, "attribute")){
                        Element value = XMLUtils.getChildByName(attribute, "value");
                        String attrKey = attribute.attributeValue("class");
                        if (value != null){
                            //TODO check if these are a different value before overwriting
                            if (attrKey.equals("Submission Title")){
                                st.msi.submissionTitle = value.getTextTrim();
                            } else if (attrKey.equals("Submission Identifier")){
                                st.msi.submissionIdentifier = value.getTextTrim();
                            } else if (attrKey.equals("Submission Description")){
                                st.msi.submissionDescription = value.getTextTrim();
                            } else if (attrKey.equals("Submission Version")){
                                st.msi.submissionVersion = value.getTextTrim();
                            } else if (attrKey.equals("Submission Reference Layer")){
                                if (value.getTextTrim().equals("true")){
                                    st.msi.submissionReferenceLayer = true;
                                } else {
                                    st.msi.submissionReferenceLayer = false;   
                                }
                            } else if (attrKey.equals("Submission Release Date")){
                                synchronized (simpledateformat){
                                    try {
                                        st.msi.submissionReleaseDate = simpledateformat.parse(value.getTextTrim());
                                    } catch (java.text.ParseException e) {
                                        log.warn("Unable to parse release date "+value.getTextTrim());
                                    }
                                }
                            } else if (attrKey.equals("Submission Update Date") || attrKey.equals("Submission Modification Date") ){
                                synchronized (simpledateformat){
                                    try {
                                        st.msi.submissionUpdateDate = simpledateformat.parse(value.getTextTrim());
                                    } catch (java.text.ParseException e) {
                                        log.warn("Unable to parse update date "+value.getTextTrim());
                                    }
                                }
                            } else if (attrKey.equals("Organizations")){
                                for (Element org : XMLUtils.getChildrenByName(value, "object")){
                                    Organization o = parseOrganization(org);
                                    st.msi.organizations.add(o);
                                }
                            } else if (attrKey.equals("Persons")){
                                for (Element per : XMLUtils.getChildrenByName(value, "object")){
                                    Person o = parsePerson(per);
                                    st.msi.persons.add(o);
                                }
                            } else if (attrKey.equals("Publications")){
                                for (Element pub : XMLUtils.getChildrenByName(value, "object")){
                                    Publication o = parsePublication(pub);
                                    st.msi.publications.add(o);
                                }
                            } else if (attrKey.equals("Term Sources")){
                                for (Element ts : XMLUtils.getChildrenByName(value, "object")){
                                    TermSource o = parseTermSource(ts);
                                    st.msi.termSources.add(o);
                                }
                            } else if (attrKey.equals("Databases")){
                                for (Element db : XMLUtils.getChildrenByName(value, "object")){
                                    Database o = parseDatabase(db);
                                    st.msi.databases.add(o);
                                }
                            } else {
                                log.warn("Unrecognised attribute "+attrKey+":"+value.getTextTrim());
                            }
                            //TODO group nodes and attributes
                        }
                    }
                    
                    //now process samples
                    for (Element sample : XMLUtils.getChildrenByName(group, "Sample")){
                        SampleNode sampleNode = new SampleNode();
                        sampleNode.setSampleAccession(sample.attributeValue("id"));

                        
                        for (Element attribute : XMLUtils.getChildrenByName(sample, "attribute")){
                            Element value = XMLUtils.getChildByName(attribute, "value");
                            String attrKey = attribute.attributeValue("class");
                            if (value != null){
                                //TODO check if these are a different value before overwriting
                                if (attrKey.equals("Name")){
                                    sampleNode.setNodeName(value.getTextTrim());
                                } else if (attrKey.equals("Sample Description")){
                                    sampleNode.setSampleDescription(value.getTextTrim());
                                } else if (attrKey.equals("Material")){
                                    AbstractNodeAttributeOntology a = new MaterialAttribute(value.getTextTrim());
                                    applyTermSource(value, a);
                                    sampleNode.addAttribute(a);
                                } else if (attrKey.equals("Sex")){
                                    AbstractNodeAttributeOntology a = new SexAttribute(value.getTextTrim());
                                    applyTermSource(value, a);
                                    sampleNode.addAttribute(a);
                                } else if (attrKey.equals("Organism")){
                                    AbstractNodeAttributeOntology a = new OrganismAttribute(value.getTextTrim());
                                    applyTermSource(value, a);
                                    sampleNode.addAttribute(a);
                                } else if (attrKey.startsWith("characteristic[")){
                                    String charType = attrKey.substring(15,attrKey.length()-1);
                                    AbstractNodeAttributeOntology a = new CharacteristicAttribute(charType, value.getTextTrim());
                                    applyTermSource(value, a);
                                    sampleNode.addAttribute(a);
                                } else if (attrKey.startsWith("comment[")){
                                    String charType = attrKey.substring(8,attrKey.length()-1);
                                    AbstractNodeAttributeOntology a = new CommentAttribute(charType, value.getTextTrim());
                                    applyTermSource(value, a);
                                    sampleNode.addAttribute(a);
                                } else if (attrKey.equals("Child Of")){
                                    sampleNode.addAttribute(new ChildOfAttribute(value.getTextTrim()));
                                } else if (attrKey.equals("Same As")){
                                    sampleNode.addAttribute(new SameAsAttribute(value.getTextTrim()));
                                } else if (attrKey.equals("Derived From")){
                                    sampleNode.addAttribute(new DerivedFromAttribute(value.getTextTrim()));
                                } 
                            }
                        }
                        try {
                            st.scd.addNode(sampleNode);
                        } catch (ParseException e) {
                            log.error("Unable to add sample "+sampleNode, e);
                        }
                    }
                }
                
                
            } catch (FileNotFoundException e) {
                log.error("Unable to access "+inputFile, e);
            } catch (DocumentException e) {
                log.error("Unable to parse "+inputFile, e);
            }
            
        }
        
        //write it out to disk
        
        File outputFile = new File(outputFilename);
        FileWriter out = null;
        SampleTabWriter sampletabwriter = null;
        try {
            try {
                out = new FileWriter(outputFile);
            } catch (IOException e) {
                log.error("Error opening " + outputFile, e);
                return;
            }
    
            sampletabwriter = new SampleTabWriter(out);
            try {
                sampletabwriter.write(st);
                sampletabwriter.close();
                log.info("wrote to "+outputFilename);
            } catch (IOException e) {
                log.error("Error writing " + outputFile, e);
                return;
            }
        } finally {
            if (out != null){
                try {
                    out.close();
                } catch (IOException e) {
                    //do nothing
                }
                out = null;
            }
        }
            
    }
}
