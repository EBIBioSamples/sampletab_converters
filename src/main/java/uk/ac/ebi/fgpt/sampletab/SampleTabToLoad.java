package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URL;
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.datamodel.MAGETABInvestigation;
import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.parser.MAGETABParser;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.GroupNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SCDNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.NamedAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;

public class SampleTabToLoad {

    public static final SampleTabParser<SampleData> parser = new SampleTabParser<SampleData>();

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

        // All samples must be in a group
        // so create a new group and add all samples to it
        // TODO check there is not an existing group first...
        GroupNode group = new GroupNode("Other Group");
        for (SCDNode sample : sampledata.scd.getNodes(SampleNode.class)) {
            log.info("Adding sample " + sample.getNodeName() + " to group " + group.getNodeName());
            group.addSample(sample);
        }

        sampledata.scd.addNode(group);
        log.info("Added group node");
        // also need to accession the new node

        // Copy msi information on to the group node
        // group.addAttribute(new NamedAttribute("Submission Title", sampledata.msi.submissionTitle));
        group.addAttribute(new NamedAttribute("Submission Description", sampledata.msi.submissionDescription));
        // group.addAttribute(new NamedAttribute("Submission Identifier", sampledata.msi.submissionIdentifier));
        // group.addAttribute(new NamedAttribute("Submission Release Date", sampledata.msi.getSubmissionReleaseDateAsString()));
        // group.addAttribute(new NamedAttribute("Submission Update Date", sampledata.msi.getSubmissionUpdateDateAsString()));
        // group.addAttribute(new NamedAttribute("Submission Version", sampledata.msi.submissionVersion));
        // group.addAttribute(new NamedAttribute("Submission Reference Layer", sampledata.msi.submissionReferenceLayer.toString()));
        // Have to do this for each group of tags (Person *, Database *, etc)
        // and complete each individual in each group before starting the next one
        // E.g. Person Last Name, Person First Name, Person Last Name, Person First Name
        // not E.g. Person Last Name, Person Last Name, Person First Name, Person First Name
        for (int i = 0; i < sampledata.msi.personLastName.size(); i++) {
            if (i < sampledata.msi.personInitials.size()) {
                group.addAttribute(new NamedAttribute("Person First Name", sampledata.msi.personFirstName.get(i)));
            }
            if (i < sampledata.msi.personInitials.size()) {
                group.addAttribute(new NamedAttribute("Person Mid Initials", sampledata.msi.personInitials.get(i)));
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
        for (int i = 0; i < sampledata.msi.publicationDOI.size(); i++) {
            group.addAttribute(new NamedAttribute("Publication DOI", sampledata.msi.publicationDOI.get(i)));
            if (i < sampledata.msi.publicationPubMedID.size()) {
                group.addAttribute(new NamedAttribute("Publication PubMed ID", sampledata.msi.publicationPubMedID
                        .get(i)));
            }
        }
        for (int i = 0; i < sampledata.msi.termSourceName.size(); i++) {
            group.addAttribute(new NamedAttribute("Term Source Name", sampledata.msi.termSourceName.get(i)));
            group.addAttribute(new NamedAttribute("Term Source URI", sampledata.msi.termSourceURI.get(i)));
            if (i < sampledata.msi.termSourceVersion.size()) {
                group.addAttribute(new NamedAttribute("Term Source Version", sampledata.msi.termSourceVersion.get(i)));
            }
        }
        for (int i = 0; i < sampledata.msi.databaseName.size(); i++) {
            group.addAttribute(new NamedAttribute("Database Name", sampledata.msi.databaseName.get(i)));
            group.addAttribute(new NamedAttribute("Database URI", sampledata.msi.databaseURI.get(i)));
            group.addAttribute(new NamedAttribute("Database ID", sampledata.msi.databaseID.get(i)));
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

    public static void main(String[] args) {

        // manager for command line arguments
        Options options = new Options();

        // individual option and required arguments

        options.addOption("h", "help", false, "print this message and exit");

        Option option = new Option("i", "input", true, "input SampleTab filename");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("o", "output", true, "output SampleTab filename");
        option.setRequired(true);
        options.addOption(option);

        // need information to connect to database to accession new groups
        option = new Option("n", "hostname", true, "hostname of accesion MySQL database");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("t", "port", true, "port of accesion MySQL database");
        options.addOption(option);

        option = new Option("d", "database", true, "database of accesion MySQL database");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("u", "username", true, "username of accesion MySQL database");
        // option.setRequired(true);
        options.addOption(option);

        option = new Option("p", "password", true, "password of accesion MySQL database");
        option.setRequired(true);
        options.addOption(option);

        CommandLineParser parser = new GnuParser();
        CommandLine line;
        try {
            line = parser.parse(options, args);
        } catch (org.apache.commons.cli.ParseException e) {
            System.err.println("Parsing command line failed. " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("", options);
            System.exit(100);
            return;
        }

        if (line.hasOption("help")) {
            // automatically generate the help statement
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ant", options);
            return;
        }

        String inputFilename = line.getOptionValue("input");
        String outputFilename = line.getOptionValue("output");
        // this is for the accessioner database
        String hostname = line.getOptionValue("hostname");
        int port = 3306;
        if (line.hasOption("port")) {
            port = new Integer(line.getOptionValue("port"));
        }
        String database = line.getOptionValue("database");
        String username = line.getOptionValue("username");
        String password = line.getOptionValue("password");

        SampleTabToLoad toloader = new SampleTabToLoad();

        // do initial parsing and conversion
        SampleData st = null;
        try {
            st = toloader.convert(inputFilename);
        } catch (ParseException e) {
            System.err.println("ParseException converting " + inputFilename);
            e.printStackTrace();
            System.exit(121);
            return;
        } catch (IOException e) {
            System.err.println("IOException converting " + inputFilename);
            e.printStackTrace();
            System.exit(122);
            return;
        }

        // connect to accessioning database
        SampleTabAccessioner accessioner = new SampleTabAccessioner();
        try {
            accessioner.makeConnection(hostname, port, database, username, password);
        } catch (ClassNotFoundException e) {
            System.err.println("ClassNotFoundException connecting to " + hostname + ":" + port + "/" + database);
            e.printStackTrace();
            System.exit(111);
            return;
        } catch (SQLException e) {
            System.err.println("SQLException connecting to " + hostname + ":" + port + "/" + database);
            e.printStackTrace();
            System.exit(112);
            return;
        }

        // assign accession to any created groups
        try {
            st = accessioner.convert(st);
        } catch (ParseException e) {
            System.err.println("ParseException converting " + inputFilename);
            e.printStackTrace();
            System.exit(121);
            return;
        } catch (SQLException e) {
            System.err.println("SQLException converting " + inputFilename);
            e.printStackTrace();
            System.exit(123);
            return;
        }

        // write back out
        FileWriter out = null;
        try {
            out = new FileWriter(outputFilename);
        } catch (IOException e) {
            System.out.println("Error opening " + outputFilename);
            e.printStackTrace();
            System.exit(131);
            return;
        }

        SampleTabWriter sampletabwriter = new SampleTabWriter(out);
        try {
            sampletabwriter.write(st);
        } catch (IOException e) {
            System.out.println("Error writing " + outputFilename);
            e.printStackTrace();
            System.exit(141);
            return;
        }

    }
}
