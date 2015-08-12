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

    private Logger log = LoggerFactory.getLogger(getClass());
    
    public SampleTabToLoad() {
    	//do nothing
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
        
        //make sure the title is an acceptable size
        if (sampledata.msi.submissionTitle.length() > 250) {
        	sampledata.msi.submissionTitle = sampledata.msi.submissionTitle.substring(0, 250)+" [truncated]";
        }
        
        //replace implicit derived from with explicit derived from relationships
        for (SampleNode sample : sampledata.scd.getNodes(SampleNode.class)) {
            if (sample.getParentNodes().size() > 0) {
                for (Node parent : new HashSet<Node>(sample.getParentNodes())) {
                    if (SampleNode.class.isInstance(parent)) {
                        SampleNode parentsample = (SampleNode) parent;
                        DerivedFromAttribute attr = new DerivedFromAttribute(parentsample.getSampleAccession());
                        sample.addAttribute(attr);
                        sample.removeParentNode(parentsample);
                        parentsample.removeChildNode(sample);
                    }
                }
            }
        }
                
        //add a link to where the file will be available on the FTP site
        //TODO how to handle this for single samples?
        for (GroupNode group : sampledata.scd.getNodes(GroupNode.class)) {
            CommentAttribute ftpattrib = new CommentAttribute("SampleTab FTP location", 
                    "ftp://ftp.ebi.ac.uk/pub/databases/biosamples/"+
                    SampleTabUtils.getSubmissionDirPath(sampledata.msi.submissionIdentifier)+
                    "/sampletab.txt"
                );
            group.addAttribute(ftpattrib);
        }
        
        //If there was an NCBI BioSamples accession as a synonym, set it as the accession
        //unless we are already using a NCBI accession
        for (SampleNode sample : sampledata.scd.getNodes(SampleNode.class)) {
        	if (sample.getSampleAccession().matches("SAMN[0-9]*")) {
        		//already a NCBI accession, so don't try to update it
        		continue;
        	}
        	
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
        
        for (int i = 0; i < sampledata.msi.publications.size(); i++) {
            Publication p = sampledata.msi.publications.get(i);

            //PMIDs must be integers, even if source says otherwise
            Integer number = null;
            try {
                number = Integer.parseInt(p.getPubMedID());
            } catch (NumberFormatException e) {
                log.warn("Non-numeric pubmedid "+p.getPubMedID());
            }
            if (number == null) {
                p = new Publication(null, p.getDOI());
            } else {
                p = new Publication(number.toString(), p.getDOI());
            }

            //discard DOIs that aren't sane
            if (p.getDOI() == null || !p.getDOI().matches("^.+/.+$")) {
                p = new Publication(null, null);
            } else if (!p.getDOI().startsWith("doi:")) {
                p = new Publication(p.getPubMedID(), "doi:"+p.getDOI());
            }
            
            
            sampledata.msi.publications.set(i, p);
        }
        
        log.info("completed initial conversion, re-accessioning...");
                
        return sampledata;
    }

    public void convert(SampleData st, Writer writer) throws IOException, ParseException, ClassNotFoundException, SQLException {
        st = convert(st);
        log.info("sampletab converted, preparing to output");
        SampleTabWriter sampletabwriter = new SampleTabWriter(writer);
        log.info("created SampleTabWriter, preparing to write");
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
