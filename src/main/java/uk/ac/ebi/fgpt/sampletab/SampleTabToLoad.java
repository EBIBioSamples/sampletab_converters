package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.datamodel.graph.Node;
import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Database;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Publication;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.GroupNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.DerivedFromAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;

public class SampleTabToLoad {
    
    private final Accessioner accessioner;

    private Logger log = LoggerFactory.getLogger(getClass());
    
    public SampleTabToLoad(String host, int port, String database, String username, String password)
            throws ClassNotFoundException, SQLException {
        // Setup the connection with the DB
        accessioner = new Accessioner(host, port, database, username, password);
    }

    public SampleTabToLoad(Accessioner accessioner)
            throws ClassNotFoundException {
        this.accessioner = accessioner;
    }
    
    public Logger getLog() {
        return log;
    }

    public SampleData convert(String sampleTabFilename) throws IOException, ParseException, ClassNotFoundException, SQLException {
        return convert(new File(sampleTabFilename));
    }

    public SampleData convert(File sampleTabFile) throws IOException, ParseException, ClassNotFoundException, SQLException {
        SampleTabSaferParser stParser = new SampleTabSaferParser();
        return convert(stParser.parse(sampleTabFile));
    }

    public SampleData convert(SampleData sampledata) throws ParseException, SQLException, ClassNotFoundException {

        // this is stuff for loading to BioSD
        // not actually part of SampleTab spec
        
        //make sure the msi contains no duplicates
        sampledata.msi.databases = new ArrayList<Database>(new HashSet<Database>(sampledata.msi.databases));
        sampledata.msi.publications = new ArrayList<Publication>(new HashSet<Publication>(sampledata.msi.publications));
        sampledata.msi.termSources = new ArrayList<TermSource>(new HashSet<TermSource>(sampledata.msi.termSources));
        
        //must have a description
        if (sampledata.msi.submissionDescription == null || sampledata.msi.submissionDescription.trim().length() == 0) {
            sampledata.msi.submissionDescription = sampledata.msi.submissionTitle;
        }
        
        //replace implicit derived from with explicit derived from relationships
        for (SampleNode sample : sampledata.scd.getNodes(SampleNode.class)) {
            if (sample.getParentNodes().size() > 0){
                for (Node parent : new HashSet<Node>(sample.getParentNodes())){
                    if (SampleNode.class.isInstance(parent)){
                        SampleNode parentsample = (SampleNode) parent;
                        DerivedFromAttribute attr = new DerivedFromAttribute(parentsample.getSampleAccession());
                        sample.addAttribute(attr);
                        sample.removeParentNode(parentsample);
                        parentsample.removeChildNode(sample);
                    }
                }
            }
        }
        
        // All samples must be in a group
        // so create a new group and add all non-grouped samples to it
        GroupNode othergroup = new GroupNode("Other Group");
        for (SampleNode sample : sampledata.scd.getNodes(SampleNode.class)) {
            // check there is not an existing group first...
            boolean inGroup = false;
            //even if it has child nodes, both parent and child must be in a group
            //this will lead to some weird looking row duplications, but since this is an internal 
            //intermediate file it is not important
            //Follow up: since implicit derived from relationships are made explicit above, 
            //this is not an issue any more
            for (Node n : sample.getChildNodes()) {
               if (GroupNode.class.isInstance(n)) {
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
            sampledata.scd.addNode(othergroup);
            log.info("Added Other group node");
            // also need to accession the new node
        }
        
        //add a link to where the file will be avaliable on the FTP site
        for (GroupNode group : sampledata.scd.getNodes(GroupNode.class)) {
            CommentAttribute ftpattrib = new CommentAttribute("SampleTab FTP location", 
                    "ftp://ftp.ebi.ac.uk/pub/databases/biosamples/"+
                    SampleTabUtils.getSubmissionDirPath(sampledata.msi.submissionIdentifier)+
                    "/sampletab.txt"
                );
            group.addAttribute(ftpattrib);
        }
        
        //If there was an NCBI BioSamples accession as a synonym
        for (SampleNode sample : sampledata.scd.getNodes(SampleNode.class)) {
            for (SCDNodeAttribute a : sample.getAttributes()) {
                boolean isComment;
                synchronized (CommentAttribute.class) {
                    isComment = CommentAttribute.class.isInstance(a);
                }
                if (isComment) {
                    CommentAttribute ca = (CommentAttribute) a;
                    if (ca.type.toLowerCase().equals("synonym") && ca.getAttributeValue().matches("SAMN[0-9]*")) {
                        String oldAccession = sample.getSampleAccession();
                        sample.setSampleAccession(ca.getAttributeValue());
                        ca.setAttributeValue(oldAccession);
                    }
                }
            }
        }
        
        
        log.info("completed initial conversion, re-accessioning...");
        
        // assign accession to any created groups
        sampledata = accessioner.convert(sampledata);
        
        return sampledata;
    }

    public void convert(SampleData st, Writer writer) throws IOException, ParseException, ClassNotFoundException, SQLException {
        st = convert(st);
        getLog().info("sampletab converted, preparing to output");
        SampleTabWriter sampletabwriter = new SampleTabWriter(writer);
        getLog().info("created SampleTabWriter, preparing to write");
        sampletabwriter.write(st);
        sampletabwriter.close();
    }

    public void convert(SampleData st, String ouputfilename) throws IOException, ParseException, ClassNotFoundException, SQLException {
        convert(st, new File(ouputfilename));
    }

    public void convert(SampleData st, File outfile) throws IOException, ParseException, ClassNotFoundException, SQLException {
        convert(st, new FileWriter(outfile));
    }

    public void convert(File infile, File outfile) throws IOException, ParseException, ClassNotFoundException, SQLException {
        SampleTabSaferParser stParser = new SampleTabSaferParser();
        convert(stParser.parse(infile), outfile);
    }

    public void convert(String infilename, String outfilename) throws IOException, ParseException, ClassNotFoundException, SQLException {
        convert(new File(infilename), new File(outfilename));
    }
}
