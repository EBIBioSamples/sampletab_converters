package uk.ac.ebi.fgpt.sampletab;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
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

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SCDNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.utils.ENAUtils;
import uk.ac.ebi.fgpt.sampletab.utils.PersistentLookup;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class NCBISampleTabCombiner {

    private Logger log = LoggerFactory.getLogger(getClass());

    private int maxident = 1000000; // max is 1,000,000
    private static File rootdir;

    private final Map<String, Collection<File>> groupings;

    // singleton instance
    private static final NCBIBiosampleToSampleTab converter = NCBIBiosampleToSampleTab.getInstance();

    private NCBISampleTabCombiner() {
        // private constructor

        rootdir = new File("ncbicopy");
        groupings = new ConcurrentHashMap<String, Collection<File>>();

    }

    private File getFileByIdent(int ident) {
        File subdir = new File(rootdir, "" + ((ident / 1000) * 1000));
        File xmlfile = new File(subdir, "" + ident + ".xml");
        return xmlfile;
    }

    public Map<String, Collection<File>> getGroupings() throws DocumentException, SQLException {

        class GroupIDsTask implements Runnable {
            private final int ident;
            private final Map<String, Collection<File>> groupings;
            private Logger log = LoggerFactory.getLogger(getClass());

            GroupIDsTask(int ident, Map<String, Collection<File>> groupings) {
                this.ident = ident;
                this.groupings = groupings;
            }

            public Collection<String> getGroupIds(File xmlFile) throws DocumentException {

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
                    try {
                        groupids = getGroupIds(getFileByIdent(this.ident));
                    } catch (DocumentException e) {
                        e.printStackTrace();
                        return;
                    }

                    for (String groupid : groupids) {
                        Collection<File> group;
                        if (this.groupings.containsKey(groupid)) {
                            group = this.groupings.get(groupid);
                        } else {
                            group = new ConcurrentSkipListSet<File>();
                            this.groupings.put(groupid, group);
                        }
                        group.add(xmlFile);
                    }
                }
            }
        }
        ;

        int nocpus = Runtime.getRuntime().availableProcessors();
        log.info("Using " + nocpus + " threads");
        ExecutorService pool = Executors.newFixedThreadPool(nocpus);
        for (int i = 0; i < maxident; i++) {
            pool.submit(new GroupIDsTask(i, groupings));
        }
        log.info("All tasks submitted");

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
                            log.info("Using description from sample");
                            sampleout.msi.submissionDescription = ((SampleNode) node).sampleDescription;
                        if (sampleout.msi.submissionTitle == null || sampleout.msi.submissionTitle.trim().equals(""))
                            log.info("Using title from sample");
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

    public void combine() {

        Map<String, Collection<File>> groups;

        try {
            log.info("Getting groupings...");
            groups = getGroupings();
            log.info("Got groupings...");
        } catch (DocumentException e) {
            log.warn("Unable to group");
            e.printStackTrace();
            return;
        } catch (SQLException e) {
            log.warn("Unable to group");
            e.printStackTrace();
            return;
        }

        for (String group : groups.keySet()) {

            if (group == null || group.equals("")) {
                log.info("Skipping empty group name");
                continue;
            }
            if (groups.get(group).size() < 1) {
                continue;
            }

            log.info("Size of group : " + group + " : " + groups.get(group).size());

            // log.info("Group : " + group);
            Collection<SampleData> sampledatas = new ArrayList<SampleData>();
            for (File xmlfile : groups.get(group)) {
                // log.debug("Group: " + group + " Filename: " + xmlfile);
                log.info("converting "+xmlfile);
                try {
                    sampledatas.add(converter.convert(xmlfile));
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
                }
            }

            SampleData sampleout = combine(sampledatas);
            // more sanity checks
            if (sampleout.scd.getRootNodes().size() != groups.get(group).size()) {
                log.warn("unequal size of group " + group + " : " + sampleout.scd.getRootNodes().size() + " vs "
                        + groups.get(group).size());
            }

            sampleout.msi.submissionIdentifier = "GNC-" + group;

            File outdir = new File("output");
            File outsubdir = new File(outdir, group);
            outsubdir.mkdirs();

            if (sampleout.scd.getNodeCount() > 0) {
                // dont bother outputting if there are no samples...
                File outfile = new File(outsubdir, "sampletab.txt");
                SampleTabWriter writer;

                try {
                    writer = new SampleTabWriter(new BufferedWriter(new FileWriter(outfile)));
                    writer.write(sampleout);
                    writer.close();
                } catch (IOException e) {
                    log.warn("Unable to write " + outfile);
                    e.printStackTrace();
                    continue;
                }
            }
        }
    }

    public static void main(String[] args) {
        NCBISampleTabCombiner instance = new NCBISampleTabCombiner();

        if (args.length == 0) {
            // no args, so search and combine ones
            instance.log.info("Starting combiner...");
            instance.combine();
        } else {
            // convert and combine the given xml files
            Collection<File> infiles = new ArrayList<File>();
            for (int i = 0; i < args.length; i++) {
                File infile = new File(args[i]);
                infile = infile.getAbsoluteFile();
                if (infile.exists())
                    System.out.println("Combining " + infile);
                infiles.add(infile);
            }
            Collection<SampleData> sampledatas = new ArrayList<SampleData>();
            for (File xmlfile : infiles) {
                SampleData sampledata;
                try {
                    sampledata = converter.convert(xmlfile);
                } catch (DocumentException e) {
                    System.err.println("Unable to convert " + xmlfile);
                    e.printStackTrace();
                    return;
                } catch (ParseException e) {
                    System.err.println("Unable to convert " + xmlfile);
                    e.printStackTrace();
                    return;
                } catch (uk.ac.ebi.arrayexpress2.magetab.exception.ParseException e) {
                    System.err.println("Unable to convert " + xmlfile);
                    e.printStackTrace();
                    return;
                }
                sampledatas.add(sampledata);
            }

            SampleData outdata = instance.combine(sampledatas);

            File outfile = new File("sampletab.txt").getAbsoluteFile();
            //TODO hard coded, bad!
            outdata.msi.submissionIdentifier = "GNC-DRP000001";
           
            SampleTabWriter writer;

            try {
                writer = new SampleTabWriter(new BufferedWriter(new FileWriter(outfile)));
                writer.write(outdata);
                writer.close();
            } catch (IOException e) {
                System.err.println("Unable to write " + outfile);
                e.printStackTrace();
                return;
            }
            System.out.println("Writen to " + outfile);
        }

    }

}
