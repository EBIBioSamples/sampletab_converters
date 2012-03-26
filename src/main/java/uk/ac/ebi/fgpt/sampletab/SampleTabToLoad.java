package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Database;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Publication;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.GroupNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SCDNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.AbstractNamedAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.NamedAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;

public class SampleTabToLoad {

    @Option(name = "-h", usage = "display help")
    private boolean help;

    @Option(name = "-i", aliases={"--input"}, usage = "input filename or glob")
    private String inputFilename;

    @Option(name = "-o", aliases={"--output"}, usage = "output filename relative to input")
    private String outputFilename;

    @Option(name = "-n", aliases={"--hostname"}, usage = "server hostname")
    private String hostname;

    @Option(name = "-t", aliases={"--port"}, usage = "server port")
    private int port = 3306;

    @Option(name = "-d", aliases={"--database"}, usage = "server database")
    private String database;

    @Option(name = "-u", aliases={"--username"}, usage = "server username")
    private String username;

    @Option(name = "-p", aliases={"--password"}, usage = "server password")
    private String password;

    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;

    // receives other command line parameters than options
    @Argument
    private List<String> arguments = new ArrayList<String>();

    private final SampleTabParser<SampleData> parser = new SampleTabParser<SampleData>();

    public SampleTabToLoad() {
        // do nothing
    }

    public SampleTabToLoad(String host, int port, String database, String username, String password)
            throws ClassNotFoundException {
        this();
        // Setup the connection with the DB
        this.username = username;
        this.password = password;
        this.hostname = host;
        this.port = port;
        this.database = database;
    }
    
    public Logger getLog() {
        return log;
    }

    // logging
    private Logger log = LoggerFactory.getLogger(getClass());

    public SampleData convert(String sampleTabFilename) throws IOException, ParseException {
        return convert(new File(sampleTabFilename));
    }

    public SampleData convert(File sampleTabFile) throws IOException, ParseException {
        return convert(parser.parse(sampleTabFile));
    }

    public SampleData convert(SampleData sampledata) throws ParseException {

        // this is stuff for loading to BioSD
        // not actually part of SampleTab spec
        
        //make sure the msi contains no duplicates
        sampledata.msi.databases = new ArrayList<Database>(new HashSet<Database>(sampledata.msi.databases));
        sampledata.msi.publications = new ArrayList<Publication>(new HashSet<Publication>(sampledata.msi.publications));
        sampledata.msi.termSources = new ArrayList<TermSource>(new HashSet<TermSource>(sampledata.msi.termSources));
        
        //must have a description
        if (sampledata.msi.submissionDescription == null || sampledata.msi.submissionDescription.trim().length() == 0){
            sampledata.msi.submissionDescription = sampledata.msi.submissionTitle;
        }
        
        // All samples must be in a group
        // so create a new group and add all samples to it
        // TODO check there is not an existing group first...
        GroupNode group = new GroupNode("Other Group");
        for (SCDNode sample : sampledata.scd.getNodes(SampleNode.class)) {
            log.debug("Adding sample " + sample.getNodeName() + " to group " + group.getNodeName());
            group.addSample(sample);
        }

        sampledata.scd.addNode(group);
        log.debug("Added group node");
        // also need to accession the new node

        // Copy msi information on to the group node
        group.addAttribute(new NamedAttribute("Submission Title", sampledata.msi.submissionTitle));
        group.addAttribute(new NamedAttribute("Submission Description", sampledata.msi.submissionDescription));
        group.addAttribute(new NamedAttribute("Submission Identifier", sampledata.msi.submissionIdentifier));
        group.addAttribute(new NamedAttribute("Submission Release Date", sampledata.msi.getSubmissionReleaseDateAsString()));
        group.addAttribute(new NamedAttribute("Submission Update Date", sampledata.msi.getSubmissionUpdateDateAsString()));
        group.addAttribute(new NamedAttribute("Submission Version", sampledata.msi.submissionVersion));
        group.addAttribute(new NamedAttribute("Submission Reference Layer", sampledata.msi.submissionReferenceLayer.toString()));
        // Have to do this for each group of tags (Person *, Database *, etc)
        // and complete each individual in each group before starting the next one
        // E.g. Person Last Name, Person First Name, Person Last Name, Person First Name
        // not E.g. Person Last Name, Person Last Name, Person First Name, Person First Name
        for (int i = 0; i < sampledata.msi.personLastName.size(); i++) {
            if (i < sampledata.msi.personInitials.size()) {
                group.addAttribute(new NamedAttribute("Person First Name", sampledata.msi.personFirstName.get(i)));
            }
            if (i < sampledata.msi.personInitials.size()) {
                group.addAttribute(new NamedAttribute("Person Initials", sampledata.msi.personInitials.get(i)));
            }
            group.addAttribute(new NamedAttribute("Person Last Name", sampledata.msi.personLastName.get(i)));
            if (i < sampledata.msi.personEmail.size()) {
                group.addAttribute(new NamedAttribute("Person Email", sampledata.msi.personEmail.get(i)));
            }
            if (i < sampledata.msi.personRole.size()) {
                group.addAttribute(new NamedAttribute("Person Role", sampledata.msi.personRole.get(i)));
            }
        }
        for (int i = 0; i < sampledata.msi.organizationName.size(); i++) {
            group.addAttribute(new NamedAttribute("Organization Name", sampledata.msi.organizationName.get(i)));
            if (i < sampledata.msi.organizationURI.size()) {
                group.addAttribute(new NamedAttribute("Organization Address", sampledata.msi.organizationAddress.get(i)));
            }
            if (i < sampledata.msi.organizationURI.size()) {
                group.addAttribute(new NamedAttribute("Organization URI", sampledata.msi.organizationURI.get(i)));
            }
            if (i < sampledata.msi.organizationEmail.size()) {
                group.addAttribute(new NamedAttribute("Organization Email", sampledata.msi.organizationEmail.get(i)));
            }
            if (i < sampledata.msi.organizationRole.size()) {
                group.addAttribute(new NamedAttribute("Organization Role", sampledata.msi.organizationRole.get(i)));
            }
        }
        for(Publication pub: sampledata.msi.publications){
            if (pub.getDOI() != null && pub.getDOI().trim().length() > 0){
                group.addAttribute(new NamedAttribute("Publication DOI", pub.getDOI()));
            }
            if (pub.getPubMedID() != null && pub.getPubMedID().trim().length() > 0){
                group.addAttribute(new NamedAttribute("Publication PubMed ID", pub.getPubMedID()));
            }
        }
        for (TermSource ts : sampledata.msi.termSources) {
            if (ts.getName() != null && ts.getName().trim().length() > 0){
                group.addAttribute(new NamedAttribute("Term Source Name", ts.getName()));
                if (ts.getURI() != null && ts.getURI().trim().length() > 0){
                    group.addAttribute(new NamedAttribute("Term Source URI", ts.getURI()));
                }
                if (ts.getVersion() != null && ts.getVersion().trim().length() > 0){
                    group.addAttribute(new NamedAttribute("Term Source Version", ts.getVersion()));
                }
            }
        }
        for (Database db : sampledata.msi.databases){
            if (db.getName() != null && db.getName().trim().length() > 0){
                group.addAttribute(new NamedAttribute("Database Name", db.getName()));
                group.addAttribute(new NamedAttribute("Database URI", db.getURI()));
                group.addAttribute(new NamedAttribute("Database ID", db.getID()));
            }
        }

        return sampledata;
    }

    public void convert(SampleData st, Writer writer) throws IOException, ParseException {
        st = convert(st);
        getLog().debug("sampletab converted, preparing to output");
        SampleTabWriter sampletabwriter = new SampleTabWriter(writer);
        getLog().debug("created SampleTabWriter");
        sampletabwriter.write(st);
        sampletabwriter.close();
    }

    public void convert(SampleData st, String ouputfilename) throws IOException, ParseException {
        convert(st, new File(ouputfilename));
    }

    public void convert(SampleData st, File outfile) throws IOException, ParseException {
        convert(st, new FileWriter(outfile));
    }

    public void convert(File infile, File outfile) throws IOException, ParseException {
        convert(parser.parse(infile), outfile);
    }

    public void convert(String infilename, String outfilename) throws IOException, ParseException {
        convert(new File(infilename), new File(outfilename));
    }

    class ToLoadTask implements Runnable {
        private final File inputFile;
        private final File outputFile;

        public ToLoadTask(File inputFile, File outputFile) {
            this.inputFile = inputFile;
            this.outputFile = outputFile;
        }

        public void run() {
            log.debug("Processing " + inputFile);

            SampleData st = null;
            SampleTabToLoad toloader = new SampleTabToLoad();
            // do initial parsing and conversion
            try {
                st = toloader.convert(inputFile);
            } catch (ParseException e) {
                System.err.println("ParseException converting " + inputFile);
                e.printStackTrace();
                return;
            } catch (IOException e) {
                System.err.println("IOException converting " + inputFile);
                e.printStackTrace();
                return;
            }

            // get an accessioner and connect to database
            SampleTabAccessioner accessioner;
            try {
                accessioner = new SampleTabAccessioner(hostname, port, database, username, password);
            } catch (ClassNotFoundException e) {
                log.error("ClassNotFoundException connecting to " + hostname + ":" + port + "/" + database);
                e.printStackTrace();
                return;
            }

            // assign accession to any created groups
            try {
                st = accessioner.convert(st);
            } catch (ParseException e) {
                System.err.println("ParseException converting " + inputFile);
                e.printStackTrace();
                return;
            } catch (SQLException e) {
                System.err.println("SQLException converting " + inputFile);
                e.printStackTrace();
                return;
            }

            // write back out
            FileWriter out = null;
            try {
                out = new FileWriter(outputFile);
            } catch (IOException e) {
                System.out.println("Error opening " + outputFile);
                e.printStackTrace();
                return;
            }

            SampleTabWriter sampletabwriter = new SampleTabWriter(out);
            try {
                sampletabwriter.write(st);
            } catch (IOException e) {
                System.out.println("Error writing " + outputFile);
                e.printStackTrace();
                return;
            }

            log.debug("Processed " + inputFile);

        }
    }

    public static void main(String[] args) {
        new SampleTabToLoad().doMain(args);
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

        log.info("Looking for input files " + inputFilename);
        List<File> inputFiles = new ArrayList<File>();
        inputFiles = FileUtils.getMatchesGlob(inputFilename);
        log.info("Found " + inputFiles.size() + " input files");
        Collections.sort(inputFiles);

        int nothreads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(nothreads);

        for (File inputFile : inputFiles) {
            // System.out.println("Checking "+inputFile);
            File outputFile = new File(inputFile.getParentFile(), outputFilename);
            // TODO also compare file ages
            if (!outputFile.exists()
                    || outputFile.lastModified() < inputFile.lastModified()) {
                Runnable t = new ToLoadTask(inputFile, outputFile);
                if (threaded) {
                    pool.execute(t);
                } else {
                    t.run();
                }
            }
        }
        // run the pool and then close it afterwards
        // must synchronize on the pool object
        synchronized (pool) {
            pool.shutdown();
            try {
                // allow 24h to execute. Rather too much, but meh
                pool.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                log.error("Interupted awaiting thread pool termination");
                e.printStackTrace();
            }
        }
        log.info("Finished processing");
    }
}
