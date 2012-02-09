package uk.ac.ebi.fgpt.sampletab;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SCDNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class NCBISampleTabCombiner {

    @Option(name = "-h", usage = "display help")
    private boolean help;
    
    @Option(name = "-i", usage = "input directory")
    private String inputFilename;
    
    @Option(name = "-o", usage = "output directory")
    private String outputFilename;

    // receives other command line parameters than options
    @Argument
    private List<String> arguments = new ArrayList<String>();
    
    private Logger log = LoggerFactory.getLogger(getClass());

    private int maxident = 300000; // max is 1,000,000
    
    private final Map<String, Set<String>> groupings;

    public NCBISampleTabCombiner() {
        // private constructor
        groupings = new ConcurrentHashMap<String, Set<String>>();
    }

    private File getFileByIdent(int ident) {
        File subdir = new File(this.inputFilename, "GNC-"+ident);
        File xmlfile = new File(subdir, "raw.xml");
        return xmlfile;
    }

    public Map<String, Set<String>> getGroupings() throws DocumentException, SQLException {

        class GroupIDsTask implements Runnable {
            private final int ident;
            private final Map<String, Set<String>> groupings;
            private Logger log = LoggerFactory.getLogger(getClass());

            GroupIDsTask(int ident, Map<String, Set<String>> groupings) {
                this.ident = ident;
                this.groupings = groupings;
            }

            public Collection<String> getGroupIds(File xmlFile) throws DocumentException, FileNotFoundException {

                log.info("Trying " + xmlFile);
                Document xml = XMLUtils.getDocument(xmlFile);

                Collection<String> groupids = new ArrayList<String>();
                Element root = xml.getRootElement();
                
                Element ids = XMLUtils.getChildByName(root, "Ids");
                Element attributes = XMLUtils.getChildByName(root, "Attributes");                
                for (Element id : XMLUtils.getChildrenByName(ids, "Id")) {
                    String dbname = id.attributeValue("db");
                    String sampleid = id.getText();
                    if (dbname.equals("SRA")) {
                        // group by sra study
                        log.debug("Getting studies of SRA sample " + sampleid);
                        Collection<String> studyids = ENAUtils.getStudiesForSample(sampleid);
                        if (studyids != null) {
                            groupids.addAll(studyids);
                        }
                    } else if (dbname.equals("dbGaP")) {
                        // group by dbGaP project
                        for (Element attribute : XMLUtils.getChildrenByName(attributes, "Attribute")) {
                            if (attribute.attributeValue("attribute_name").equals("gap_accession")) {
                                groupids.add(attribute.getText());
                            }
                        }
                    } else if (dbname.equals("EST") || dbname.equals("GSS")) {
                        // EST == Expressed Sequence Tag
                        // GSS == Genome Survey Sequence
                        // group by owner
                        //
                        // Element owner = XMLUtils.getChildByName(root, "Owner");
                        // Element name = XMLUtils.getChildByName(owner, "Name");
                        // if (name != null) {
                        // String ownername = name.getText();
                        // // clean ownername
                        // ownername = ownername.toLowerCase();
                        // ownername = ownername.trim();
                        // String cleanname = "";
                        // for (int j = 0; j < ownername.length(); j++) {
                        // String c = ownername.substring(j, j + 1);
                        // if (c.matches("[a-z0-9]")) {
                        // cleanname += c;
                        // }
                        // }
                        // groupids.add(cleanname);
                        // }

                        // // This doesnt work so well by owner, so dont bother.
                        // // May need to group samples from the same owner in a post-hoc manner?
                        groupids.add(sampleid);
                    } else {
                        // could group by others, but some of them are very big
                    }
                }
                return groupids;
            }

            public void run() {

                File xmlFile = getFileByIdent(this.ident);

                if (xmlFile.exists()) {
                    Collection<String> groupids = null;
//                    try {
//                        groupids = getGroupIds(getFileByIdent(this.ident));
//                    } catch (DocumentException e) {
//                        e.printStackTrace();
//                        return;
//                    }
                    groupids = new HashSet<String>();
                    groupids.add(""+this.ident);
                    
                    if(groupids.size() > 1){
                        log.error("Multiple groups for "+this.ident);
                    }
                        
                    
                    for (String groupid : groupids) {
                        Set<String> group;
                        if (this.groupings.containsKey(groupid)) {
                            group = this.groupings.get(groupid);
                        } else {
                            group = new ConcurrentSkipListSet<String>();
                            this.groupings.put(groupid, group);
                        }
                        group.add(""+this.ident);
                    }
                }
            }
        }
        

        int nocpus = Runtime.getRuntime().availableProcessors();
        log.info("Using " + nocpus + " threads");
        ExecutorService pool = Executors.newFixedThreadPool(nocpus);
        for (int i = 0; i < maxident; i++) {
            pool.submit(new GroupIDsTask(i, groupings));
        }
        log.info("All GroupIDsTask tasks submitted");

        synchronized (pool) {
            pool.shutdown();
            try {
                // allow 24h to execute. Rather too much, but meh
                pool.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                log.error("Interuppted awaiting thread pool termination");
                e.printStackTrace();
            }
        }
        
        // at this point, subs is a mapping from the project name to a set of BioSample accessions
        // output them to a file
        File projout = new File("projects.tab.txt");
        BufferedWriter projoutwrite = null; 
        try {
            projoutwrite = new BufferedWriter(new FileWriter(projout));
            synchronized (groupings) {
                // sort them to put them in a sensible order
                List<String> projects = new ArrayList<String>(groupings.keySet());
                Collections.sort(projects);
                for (String project : projects) {

                    projoutwrite.write(project);
                    projoutwrite.write("\t");
                    List<String> accessions = new ArrayList<String>(groupings.get(project));
                    Collections.sort(accessions);
                    for (String accession : accessions) {
                        projoutwrite.write(accession);
                        projoutwrite.write("\t");
                    }

                    projoutwrite.write("\n");
                }
            }
        } catch (IOException e) {
            log.error("Unable to write to " + projout);
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (projoutwrite != null){
                try {
                    projoutwrite.close();
                } catch (IOException e) {
                    //failed within a fail so give up
                    log.error("Unable to close file writer " + projout);
                    
                }
            }
        }
        
        return groupings;

    }

    public SampleData combine(Collection<SampleData> indata) {

        SampleData sampleout = new SampleData();
        for (SampleData sampledata : indata) {

            for (int i = 0; i < sampledata.msi.organizationName.size(); i++) {
                if (!sampleout.msi.organizationName.contains(sampledata.msi.organizationName.get(i))) {

                    sampleout.msi.organizationName.add(sampledata.msi.organizationName.get(i));

                    if (i >= sampledata.msi.organizationAddress.size()) {
                        sampleout.msi.organizationAddress.add("");
                    } else {
                        sampleout.msi.organizationAddress.add(sampledata.msi.organizationAddress.get(i));
                    }

                    if (i >= sampledata.msi.organizationEmail.size()) {
                        sampleout.msi.organizationEmail.add("");
                    } else {
                        sampleout.msi.organizationEmail.add(sampledata.msi.organizationEmail.get(i));
                    }

                    if (i >= sampledata.msi.organizationRole.size()) {
                        sampleout.msi.organizationRole.add("");
                    } else {
                        sampleout.msi.organizationRole.add(sampledata.msi.organizationRole.get(i));
                    }

                    if (i >= sampledata.msi.organizationURI.size()) {
                        sampleout.msi.organizationURI.add("");
                    } else {
                        sampleout.msi.organizationURI.add(sampledata.msi.organizationURI.get(i));
                    }
                }
            }

            for (int i = 0; i < sampledata.msi.personLastName.size(); i++) {
                // TODO this assumes no same surnamed people
                if (!sampleout.msi.personLastName.contains(sampledata.msi.personLastName.get(i))) {

                    sampleout.msi.personLastName.add(sampledata.msi.personLastName.get(i));

                    if (i >= sampledata.msi.personInitials.size()) {
                        sampleout.msi.personInitials.add("");
                    } else {
                        sampleout.msi.personInitials.add(sampledata.msi.personInitials.get(i));
                    }

                    if (i >= sampledata.msi.personFirstName.size()) {
                        sampleout.msi.personFirstName.add("");
                    } else {
                        sampleout.msi.personFirstName.add(sampledata.msi.personFirstName.get(i));
                    }

                    if (i >= sampledata.msi.personEmail.size()) {
                        sampleout.msi.personEmail.add("");
                    } else {
                        sampleout.msi.personEmail.add(sampledata.msi.personEmail.get(i));
                    }

                    if (i >= sampledata.msi.personRole.size()) {
                        sampleout.msi.personRole.add("");
                    } else {
                        sampleout.msi.personRole.add(sampledata.msi.personRole.get(i));
                    }
                }
            }

            for (int i = 0; i < sampledata.msi.termSourceName.size(); i++) {
                if (!sampleout.msi.termSourceName.contains(sampledata.msi.termSourceName.get(i))) {

                    sampleout.msi.termSourceName.add(sampledata.msi.termSourceName.get(i));

                    if (i >= sampledata.msi.termSourceURI.size()) {
                        sampleout.msi.termSourceURI.add("");
                    } else {
                        sampleout.msi.termSourceURI.add(sampledata.msi.termSourceURI.get(i));
                    }

                    if (i >= sampledata.msi.termSourceVersion.size()) {
                        sampleout.msi.termSourceVersion.add("");
                    } else {
                        sampleout.msi.termSourceVersion.add(sampledata.msi.termSourceVersion.get(i));
                    }
                }
            }

            sampleout.msi.databaseID.addAll(sampledata.msi.databaseID);
            sampleout.msi.databaseName.addAll(sampledata.msi.databaseName);
            sampleout.msi.databaseURI.addAll(sampledata.msi.databaseURI);

            sampleout.msi.publicationDOI.addAll(sampledata.msi.publicationDOI);
            sampleout.msi.publicationPubMedID.addAll(sampledata.msi.publicationPubMedID);

            if (sampleout.msi.submissionDescription == null|| sampleout.msi.submissionDescription.trim().equals(""))
                sampleout.msi.submissionDescription = sampledata.msi.submissionDescription;
            if (sampleout.msi.submissionTitle == null|| sampleout.msi.submissionTitle.trim().equals(""))
                sampleout.msi.submissionTitle = sampledata.msi.submissionTitle;

            if (sampledata.msi.submissionReleaseDate != null) {
                if (sampleout.msi.submissionReleaseDate == null) {
                    sampleout.msi.submissionReleaseDate = sampledata.msi.submissionReleaseDate;
                } else {
                    // use the most recent of the two dates
                    Date datadate = sampledata.msi.submissionReleaseDate;
                    Date outdate = sampleout.msi.submissionReleaseDate;
                    if (datadate != null && outdate != null && datadate.after(outdate)) {
                        sampleout.msi.submissionReleaseDate = sampledata.msi.submissionReleaseDate;

                    }
                }
            }
            if (sampledata.msi.submissionUpdateDate != null) {
                if (sampleout.msi.submissionUpdateDate == null) {
                    sampleout.msi.submissionUpdateDate = sampledata.msi.submissionUpdateDate;
                } else {
                    // use the most recent of the two dates
                    Date datadate = sampledata.msi.submissionUpdateDate;
                    Date outdate = sampleout.msi.submissionUpdateDate;
                    if (datadate != null && outdate != null && datadate.after(outdate)) {
                        sampleout.msi.submissionUpdateDate = sampledata.msi.submissionUpdateDate;

                    }
                }
            }
            
            // add nodes from here to parent
            for (SCDNode node : sampledata.scd.getRootNodes()) {
                try {
                    sampleout.scd.addNode(node);
                    if (SampleNode.class.isInstance(node)){
                        //apply node metainformation to msi if missing
                        if (sampleout.msi.submissionDescription == null || sampleout.msi.submissionDescription.trim().equals(""))
                            log.debug("Using description from sample");
                            sampleout.msi.submissionDescription = ((SampleNode) node).sampleDescription;
                        if (sampleout.msi.submissionTitle == null || sampleout.msi.submissionTitle.trim().equals(""))
                            log.debug("Using title from sample");
                            sampleout.msi.submissionTitle = ((SampleNode) node).getNodeName();
                    }
                } catch (uk.ac.ebi.arrayexpress2.magetab.exception.ParseException e4) {
                    log.warn("Unable to add node " + node.getNodeName());
                    e4.printStackTrace();
                    continue;
                }
            }
        }

        if (sampleout.msi.submissionDescription == null || sampleout.msi.submissionDescription.trim().equals("")) {
            if (sampleout.msi.submissionDescription == null)
                log.warn("null submission description");
            sampleout.msi.submissionDescription = "No description avaliable";
        }
        if (sampleout.msi.submissionTitle == null || sampleout.msi.submissionTitle.trim().equals("")) {
            log.warn("null submission title");
            sampleout.msi.submissionTitle = sampleout.msi.submissionIdentifier;
        }

        return sampleout;
    }
    
    public static void main(String[] args) throws IOException {
        new NCBISampleTabCombiner().doMain(args);
    }

    public void doMain(String[] args) throws IOException {
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
        
        log.info("Starting combiner...");

        Map<String, Set<String>> groups;

        try {
            log.debug("Getting groupings...");
            groups = getGroupings();
            log.debug("Got groupings...");
        } catch (DocumentException e) {
            log.error("Unable to group");
            e.printStackTrace();
            return;
        } catch (SQLException e) {
            log.error("Unable to group");
            e.printStackTrace();
            return;
        }
        
        
        class OutputTask implements Runnable {
            private final Collection<String> group;
            private final String groupname;
            private final File outFile;
            private final NCBIBiosampleToSampleTab converter;
            
            public OutputTask(Collection<String> group, String groupname, File outFile){
                this.group = group;
                this.groupname = groupname;
                this.converter = new NCBIBiosampleToSampleTab();
                this.outFile = outFile;
            }
            public void run() {

                //log.info("Size of group : " + group + " : " + groups.get(group).size());

                //log.info("Group : " + group);
                Collection<SampleData> sampledatas = new ArrayList<SampleData>();
                for (String ncbiaccession : this.group) {
                    File xmlfile = getFileByIdent(new Integer(ncbiaccession));
                    log.debug("converting "+xmlfile);
                    try {
                        sampledatas.add(this.converter.convert(xmlfile));
                    } catch (ParseException e2) {
                        log.warn("Unable to convert " + xmlfile);
                        e2.printStackTrace();
                        continue;
                    } catch (uk.ac.ebi.arrayexpress2.magetab.exception.ParseException e2) {
                        log.warn("Unable to convert " + xmlfile);
                        e2.printStackTrace();
                        continue;
                    } catch (DocumentException e2) {
                        log.warn("Unable to convert " + xmlfile);
                        e2.printStackTrace();
                        continue;
                    } catch (FileNotFoundException e2) {
                        log.warn("Unable to convert " + xmlfile);
                        e2.printStackTrace();
                        continue;
                    }
                }
                SampleData sampleout = combine(sampledatas);
                
                sampleout.msi.submissionIdentifier = "GNC-" + this.groupname;
                log.debug("submissionIdentifier: "+sampleout.msi.submissionIdentifier);
                
                // more sanity checks
                if (sampleout.scd.getRootNodes().size() != this.group.size()) {
                    log.warn("unequal group size "+sampleout.msi.submissionIdentifier);
                }
                
                SampleTabWriter writer;
                try {
                    writer = new SampleTabWriter(new BufferedWriter(new FileWriter(outFile)));
                    writer.write(sampleout);
                    writer.close();
                } catch (IOException e) {
                    log.error("Unable to write " + outFile);
                    e.printStackTrace();
                    return;
                }
                
            }
        }
        
        
        // sort them to put them in a sensible order
        List<String> projects = new ArrayList<String>(groupings.keySet());
        Collections.sort(projects);
        
        int nocpus = Runtime.getRuntime().availableProcessors();
        log.info("Using " + nocpus + " threads");
        ExecutorService pool = Executors.newFixedThreadPool(nocpus*2);

        for (String groupname : projects) {

            if (groupname == null || groupname.equals("")) {
                log.info("Skipping empty group name");
                continue;
            }
            if (groups.get(groupname).size() < 1) {
                continue;
            }
            
            File outsubdir = new File(outputFilename, "GNC-"+groupname);
            File outFile = new File(outsubdir, "sampletab.txt");
            //TODO also compare file ages
            //TODO also check output file is size > 0
            if (!outFile.exists()){
                outFile.getParentFile().mkdirs();
                log.debug("outfile "+outFile);
                //TODO make this operate on inputfiles and outputfiles
                Runnable t = new OutputTask(groups.get(groupname), groupname, outFile);
                pool.execute(t);
                //t.run();        
            }
        }
        log.info("All OutputTask tasks submitted");
        synchronized (pool) {
            pool.shutdown();
            try {
                // allow 24h to execute. Rather too much, but meh
                pool.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                log.error("Interuppted awaiting thread pool termination");
                e.printStackTrace();
            }
        }

    }

}
