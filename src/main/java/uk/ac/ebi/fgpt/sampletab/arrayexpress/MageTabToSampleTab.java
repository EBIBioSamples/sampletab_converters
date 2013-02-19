package uk.ac.ebi.fgpt.sampletab.arrayexpress;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.dom4j.DocumentException;
import org.mged.magetab.error.ErrorItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.datamodel.IDF;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.MAGETABInvestigation;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.graph.Node;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.graph.utils.GraphUtils;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SDRFNode;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SourceNode;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.attribute.CharacteristicsAttribute;
import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.listener.ErrorItemListener;
import uk.ac.ebi.arrayexpress2.magetab.parser.IDFParser;
import uk.ac.ebi.arrayexpress2.magetab.parser.MAGETABParser;
import uk.ac.ebi.arrayexpress2.magetab.validator.Validator;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Database;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Organization;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Person;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Publication;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SCDNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SameAsAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.UnitAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.arrayexpress2.sampletab.validator.SampleTabValidator;
import uk.ac.ebi.fgpt.sampletab.CorrectorTermSource;
import uk.ac.ebi.fgpt.sampletab.utils.BioSDUtils;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;

public class MageTabToSampleTab {
    private final MAGETABParser<MAGETABInvestigation> parser;
    private final List<ErrorItem> errorItems;

    private SimpleDateFormat magetabdateformat = new SimpleDateFormat(
            "yyyy-MM-dd");

    // logging
    public Logger log = LoggerFactory.getLogger(getClass());

    public MageTabToSampleTab() {
        parser = new MAGETABParser<MAGETABInvestigation>();
        errorItems = new ArrayList<ErrorItem>();

        parser.addErrorItemListener(new ErrorItemListener() {

            public void errorOccurred(ErrorItem item) {
                errorItems.add(item);
            }
        });
        
    }

    public SampleData convert(String idfFilename) throws IOException, ParseException {
        return convert(new File(idfFilename));
    }

    public SampleData convert(File idfFile) throws IOException, ParseException {
        //A few IDF files specify multiple SDRF files which may not all have been downloaded.
        //Due to a bug in Limpopo, this can cause Limpopo to hang indefinitely.
        //therefore, first parse the IDF only to see if this is something to avoid.
        
        IDFParser idfparser = new IDFParser();
        IDF idf = null;
        idf = idfparser.parse(idfFile);
        if (idf.sdrfFile.size() != 1){
            throw new ParseException("Multiple sdrf file references");
        }

        errorItems.clear();
        MAGETABInvestigation mt = parser.parse(idfFile);
        if (!errorItems.isEmpty()) {
            // there are error items, print them and fail
            for (ErrorItem error : errorItems){
                log.error(error.toString());
            }
            throw new ParseException();
        }
        return convert(mt);
    }
    
    private void convertPublications(MAGETABInvestigation mt, SampleData st){
        for (int i = 0; i < mt.IDF.publicationDOI.size() || i < mt.IDF.pubMedId.size(); i++){
            String doi = null;
            if (i < mt.IDF.publicationDOI.size()){
                doi = mt.IDF.publicationDOI.get(i);
            }
            String pubmedid = null;
            if (i < mt.IDF.pubMedId.size()){
                pubmedid = mt.IDF.pubMedId.get(i);
            }
            Publication pub = new Publication(pubmedid, doi);
            if (!st.msi.publications.contains(pub)){
                st.msi.publications.add(pub);
            }
        }
    }
    
    private void convertPeople(MAGETABInvestigation mt, SampleData st){
        for (int i = 0; i < mt.IDF.personLastName.size() || 
            i < mt.IDF.personMidInitials.size() || 
            i < mt.IDF.personFirstName.size() || 
            i < mt.IDF.personEmail.size() || 
            i < mt.IDF.personRoles.size(); i++){
            String lastname = null;
            if (i < mt.IDF.personLastName.size()){
                lastname = mt.IDF.personLastName.get(i);
            }
            String initials = null;
            if (i < mt.IDF.personMidInitials.size()){
                initials = mt.IDF.personMidInitials.get(i);
            }
            String firstname = null;
            if (i < mt.IDF.personFirstName.size()){
                firstname = mt.IDF.personFirstName.get(i);
            }
            String email = null;
            if (i < mt.IDF.personEmail.size()){
                email = mt.IDF.personEmail.get(i);
            }
            // TODO fix minor spec mismatch when there are multiple roles for the
            // same person
            String role = null;
            if (i < mt.IDF.personRoles.size()){
                role = mt.IDF.personRoles.get(i);
            }
            Person org = new Person(lastname, initials, firstname, email, role);
            if (!st.msi.persons.contains(org)){
                st.msi.persons.add(org);
            }
        }   
    }

    private void convertOrganizations(MAGETABInvestigation mt, SampleData st){

        // AE doesn't really have organisations, but does have affiliations
        // st.msi.organizationURI/Email can't be mapped from ArrayExpress
        for (int i = 0; i < mt.IDF.personAffiliation.size() || 
                            i < mt.IDF.personAddress.size() || 
                            i < mt.IDF.personRoles.size(); i++){
            String name = null;
            if (i < mt.IDF.personAffiliation.size()){
                name = mt.IDF.personAffiliation.get(i);
            }
            String uri = null;
//            if (i < mt.IDF.personAddress.size()){
//                uri = mt.IDF.personAddress.get(i);
//            }
            String role = null;
            if (i < mt.IDF.personRoles.size()){
                role = mt.IDF.personRoles.get(i);
            }
            Organization org = new Organization(name, null, uri, null, role);
            if (!st.msi.organizations.contains(org)){
                st.msi.organizations.add(org);
            }
        }
    }
    
    private String convertTermSource(String name, MAGETABInvestigation mt, SampleData st) throws ParseException{
        name = name.trim();
        if (name.length()==0){
            throw new ParseException("Cannot convert blankly named term source");
        }
        for (int i = 0; i < mt.IDF.termSourceName.size(); i++){
            String tsname = null;
            if (i < mt.IDF.termSourceName.size()){
                tsname = mt.IDF.termSourceName.get(i);
            }
            if (tsname.equals(name)){
                String uri = null;
                if (i < mt.IDF.termSourceFile.size()){
                    uri = mt.IDF.termSourceFile.get(i);
                    //bulk replace for some internal URI
                    if ("http://bar.ebi.ac.uk:8080/trac/browser/branches/curator/ExperimentalFactorOntology/ExFactorInOWL/currentrelease/eforelease/efo.owl".equals(uri)){
                        uri = "http://www.ebi.ac.uk/efo/";
                    }
                }
                String version = null;
                if (i < mt.IDF.termSourceVersion.size()){
                    version = mt.IDF.termSourceVersion.get(i);
                }
                TermSource ts = new TermSource(name, uri, version);
                return st.msi.getOrAddTermSource(ts);
            }
        }
        throw new ParseException("Unable to find term source "+name);
    }
        
    private void processCharacteristics(List<CharacteristicsAttribute> characteristics, SCDNode scdnode, MAGETABInvestigation mt, SampleData st){
        for (CharacteristicsAttribute sdrfcharacteristic : characteristics) {
            
            if (sdrfcharacteristic.getAttributeValue() != null && sdrfcharacteristic.getAttributeValue().trim().length() > 0){
                CharacteristicAttribute scdcharacteristic = new CharacteristicAttribute();
            
                scdcharacteristic.type = sdrfcharacteristic.type;
                scdcharacteristic.setAttributeValue(sdrfcharacteristic
                        .getAttributeValue());
                try {
                if (sdrfcharacteristic.unit != null) {
                    scdcharacteristic.unit = new UnitAttribute();
                    if (sdrfcharacteristic.unit.termSourceREF != null 
                            && sdrfcharacteristic.unit.termSourceREF.length() > 0 
                            && sdrfcharacteristic.unit.termAccessionNumber != null
                            && sdrfcharacteristic.unit.termAccessionNumber.length() > 0){
                            scdcharacteristic.unit.setTermSourceREF(convertTermSource(sdrfcharacteristic.unit.termSourceREF, mt, st));
                        scdcharacteristic.unit.setTermSourceID(sdrfcharacteristic.unit.termAccessionNumber);
                    }
                } else {
                    if (sdrfcharacteristic.termSourceREF != null 
                            && sdrfcharacteristic.termSourceREF.length() > 0 
                            && sdrfcharacteristic.termAccessionNumber != null
                            && sdrfcharacteristic.termAccessionNumber.length() > 0){
                        scdcharacteristic.setTermSourceREF(convertTermSource(sdrfcharacteristic.termSourceREF, mt, st));
                        scdcharacteristic.setTermSourceID(sdrfcharacteristic.termAccessionNumber);
                    }
                }
                } catch (ParseException e) {
                    log.warn("Unable to handle term source", e);
                }
                scdnode.addAttribute(scdcharacteristic);
            }
        }
    }
    
    private void processComments(Map<String, List<String>> comments, SCDNode scdnode){
        if (comments != null ){
            for (String key : comments.keySet()) {
                for (String value: comments.get(key)) {
                    if (key.equals("ENA_SAMPLE")) {
                        try {
                            for (String biosdacc : BioSDUtils.getBioSDAccessionOf(value)){
                                SameAsAttribute sameas = new SameAsAttribute(biosdacc);
                                scdnode.addAttribute(sameas);
                            }
                        } catch (DocumentException e) {
                            log.error("Problem getting accessions of "+value, e);
                        }
                    } else {
                        CommentAttribute comment = new CommentAttribute();
                        comment.type = key;
                        comment.setAttributeValue(value);
                        scdnode.addAttribute(comment);
                    }
                }
            }
        }
    }
    
    public SCDNode convertNode(Node sdrfnode, MAGETABInvestigation mt, SampleData st) throws ParseException{

        boolean useable = false;
        List<CharacteristicsAttribute> characteristics = null;
        Map<String, List<String>> comments = null;
        String prefix = null;
        
        // horribly long class references due to namespace collision
        synchronized(uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SampleNode.class){
            if (uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SampleNode.class.isInstance(sdrfnode)){
                useable = true;
                uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SampleNode nodeB = (uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SampleNode) sdrfnode;
                characteristics = nodeB.characteristics;
                comments = nodeB.comments;
                prefix = "sample";
            }
        }
        
        synchronized(SourceNode.class){
            if (SourceNode.class.isInstance(sdrfnode)){
                useable = true;
                SourceNode nodeB = (SourceNode) sdrfnode;
                characteristics = nodeB.characteristics;
                comments = nodeB.comments;
                prefix = "source";
            }
        }
        
        if (useable){
            String name = sdrfnode.getNodeName();
            name = prefix+" "+name;
            SampleNode scdnode = st.scd.getNode(name, SampleNode.class);
            
            if (scdnode == null){
                scdnode = new SampleNode();
                scdnode.setNodeName(name);
                log.info("processing " + name);
    
                processCharacteristics(characteristics, scdnode, mt, st);
                
                processComments(comments, scdnode);
                            
                st.scd.addNode(scdnode);
                
                List<uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SampleNode> downstreamSamples = new ArrayList<uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SampleNode>();
                downstreamSamples.addAll(GraphUtils.findDownstreamNodes(sdrfnode, uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SampleNode.class));
                //if this is a source node with a single downstream sample
                if (prefix.equals("source") && downstreamSamples.size() == 1){
                    //combine it with the existing node
                    uk.ac.ebi.arrayexpress2.magetab.datamodel.sdrf.node.SampleNode childSDRFNode = downstreamSamples.get(0);
                    processCharacteristics(childSDRFNode.characteristics, scdnode, mt, st);
                    processComments(childSDRFNode.comments, scdnode);
                    //maybe add the samples nodes name as a synonym?
                } else {
                    //otherwise process all downstream nodes
                    for (Node childSDRFNode : downstreamSamples){
                        SCDNode childSCDNode = convertNode(childSDRFNode, mt, st);
                        if (childSCDNode != null){
                            scdnode.addChildNode(childSCDNode);
                            childSCDNode.addParentNode(scdnode);
                        }
                    }
                    
                    for (Node childSDRFNode : GraphUtils.findDownstreamNodes(sdrfnode, SourceNode.class)){
                        SCDNode childSCDNode = convertNode(childSDRFNode, mt, st);
                        if (childSCDNode != null){
                            scdnode.addChildNode(childSCDNode);
                            childSCDNode.addParentNode(scdnode);
                        }
                    }
                }
            }
            return scdnode;
        }
        return null;
    }
    
    public SampleData convert(MAGETABInvestigation mt)
            throws ParseException {

        SampleData st = new SampleData();
        st.msi.submissionTitle = mt.IDF.investigationTitle;
        st.msi.submissionDescription = mt.IDF.experimentDescription;
        if (mt.IDF.publicReleaseDate == null){
            //null release date
            //ArrayExpress defaults to private
            //so set a date in the far future
            SampleTabUtils.releaseInACentury(st);
            
        } else if (mt.IDF.publicReleaseDate != null && !mt.IDF.publicReleaseDate.trim().equals("")) {
            try{
                st.msi.submissionReleaseDate = magetabdateformat
                    .parse(mt.IDF.publicReleaseDate.trim());
            } catch (java.text.ParseException e){
                log.error("Unable to parse release date "+mt.IDF.publicReleaseDate);
            }
        }
        st.msi.submissionIdentifier = "GA" + mt.IDF.accession;
        st.msi.submissionReferenceLayer = false;
        
        convertPublications(mt, st);
        convertPeople(mt, st);
        convertOrganizations(mt,st);
        
        Database dblink = new Database("ArrayExpress", 
                "http://www.ebi.ac.uk/arrayexpress/experiments/"+ mt.IDF.accession,
                mt.IDF.accession);
        
        st.msi.databases.add(dblink);

        log.debug("Creating node names");
        for (SDRFNode sdrfnode : mt.SDRF.getRootNodes()) {
            convertNode(sdrfnode, mt, st);
        }
        
        //correct any term source issues here first
        CorrectorTermSource cts = new CorrectorTermSource();
        cts.correct(st);
        
        
        //simple sanity check to avoid generating stupid files
        if (st.scd.getAllNodes().size() == 0){
            log.error("Zero nodes converted");
            throw new ParseException("Zero nodes converted");
        }
                
        log.info("Finished convert()");
        
        return st;
    }

    public void convert(MAGETABInvestigation mt, Writer writer)
            throws IOException, ParseException {
        log.debug("recieved magetab, preparing to convert");
        SampleData st = convert(mt);
        log.debug("sampletab converted, preparing to output");
        
        Validator<SampleData> validator = new SampleTabValidator();
        validator.validate(st);
        
        SampleTabWriter sampletabwriter = new SampleTabWriter(writer);
        log.debug("created SampleTabWriter");
        sampletabwriter.write(st);
        sampletabwriter.close();

    }

    public void convert(File idfFile, Writer writer) throws IOException,
            ParseException {
        //a few idf files specify multiple sdrf files which may not all have been downloaded
        //due to a bug in limpopo, this can cause limpopo to hang indefinitely.
        //therefore, first parse the idf only to see if this is something to avoid.

        log.info("Parsing IDF");
        IDFParser idfparser = new IDFParser();
        IDF idf = null;
        idf = idfparser.parse(idfFile);
        log.info("Checking IDF");
        if (idf.sdrfFile.size() != 1){
            log.error("Non-standard sdrf file references");
            throw new ParseException();
        }
        
        errorItems.clear();
        MAGETABInvestigation mt = parser.parse(idfFile);
        if (!errorItems.isEmpty()) {
            // there are error items, print them and fail
            for (ErrorItem error : errorItems){
                log.error(error.toString());
            }
            throw new ParseException();
        }
        
        convert(mt, writer);
    }

    public void convert(File idffile, String stfilename) throws IOException,
            ParseException {
        convert(idffile, new File(stfilename));
    }

    public void convert(File idffile, File stfile) throws IOException,
            ParseException {
        convert(idffile, new FileWriter(stfile));
    }

    public void convert(String idffilename, Writer writer) throws IOException,
            ParseException {
        convert(new File(idffilename), writer);
    }

    public void convert(String idffilename, File stfile) throws IOException,
            ParseException, java.text.ParseException {
        convert(idffilename, new BufferedWriter(new FileWriter(stfile)));
    }

    public void convert(String idffilename, String stfilename)
            throws IOException, ParseException, java.text.ParseException {
        convert(idffilename, new File(stfilename));
    }

    public static void main(String[] args) {
        new MageTabToSampleTab().doMain(args);
    }

    public void doMain(String[] args) {
        if (args.length < 2) {
            System.out
                    .println("Must provide an MAGETAB IDF filename and a SampleTab output filename.");
            System.exit(1);
            return;
        }
        String idfFilename = args[0];
        String sampleTabFilename = args[1];
        
        //a few idf files specify multiple sdrf files which may not all have been downloaded
        //due to a bug in limpopo, this can cause limpopo to hang indefinitely.
        //therefore, first parse the idf only to see if this is something to avoid.

        IDFParser idfparser = new IDFParser();
        IDF idf = null;
        try {
            idf = idfparser.parse(new File(idfFilename));
        } catch (ParseException e) {
            log.error("Error parsing " + idfFilename, e);
            System.exit(1);
            return;
        }
        
        if (idf.sdrfFile.size() != 1){
            log.error("Non-standard sdrf file references");
            log.error(""+idf.sdrfFile);
            System.exit(1);
            return;
        }

        try {
            convert(idfFilename, sampleTabFilename);
        } catch (IOException e) {
            log.error("Error converting "+idfFilename, e);
            System.exit(2);
            return;
        } catch (ParseException e) {
            log.error("Error converting "+idfFilename, e);
            System.exit(3);
            return;
        } catch (java.text.ParseException e) {
            log.error("Error converting "+idfFilename, e);
            System.exit(4);
            return;
        }
    }
}
