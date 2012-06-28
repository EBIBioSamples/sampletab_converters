package uk.ac.ebi.fgpt.sampletab;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.dom.DOMDocument;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabParser;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;

public class SampleTabToHTML {

    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    private boolean help;

    @Option(name = "-i", aliases={"--input"}, usage = "input filename or glob")
    private String inputFilename;

    @Option(name = "-o", aliases={"--output"}, usage = "output filename")
    private String outputFilename;
    
    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;

    // receives other command line parameters than options
    @Argument
    private List<String> arguments = new ArrayList<String>();

    private Logger log = LoggerFactory.getLogger(getClass());
    
    
    private Document convert(SampleData sd){
        Document d = DocumentHelper.createDocument();
        Element html = d.addElement("html");
        Element head = html.addElement("head");
        Element title = head.addElement("title");
        title.setText("BioSamples Database - "+sd.msi.submissionIdentifier);
        Element body = html.addElement("body");
        Element e;
        e = body.addElement("p");
        //TODO finish
        
        return d;
    }

    private class HTMLTask implements Runnable{
        private final File inputFile;
        private final File outputFile;

        public HTMLTask(File inputFile, File outputFile){
            this.inputFile = inputFile;
            this.outputFile = outputFile;
        }
        
        public void run() {
            SampleData sd = null;
            SampleTabSaferParser parser = new SampleTabSaferParser();
            try {
                sd = parser.parse(this.inputFile);
            } catch (ParseException e) {
                log.error("Error parsing "+this.inputFile);
                e.printStackTrace();
                return;
            }
            
            Document d = convert(sd);
            
            OutputFormat format = OutputFormat.createPrettyPrint();
            XMLWriter writer;
            try {
                writer = new XMLWriter(new FileWriter(outputFile), format );
                writer.write( d );
                writer.close();
            } catch (IOException e) {
                log.error("Error writing "+this.inputFile+" to "+this.outputFile);
                e.printStackTrace();
                return;
            }
        }
        
    }
    
    public static void main(String[] args) {
        new SampleTabToHTML().doMain(args);
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
            parser.printSingleLineUsage(System.err);
            System.err.println();
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
            return;
        }

        log.debug("Looking for input files");
        List<File> inputFiles = new ArrayList<File>();
        inputFiles = FileUtils.getMatchesGlob(inputFilename);
        log.info("Found " + inputFiles.size() + " input files from "+inputFilename);
        Collections.sort(inputFiles);

        int nothreads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(nothreads);
        
        for (File inputFile : inputFiles) {
            // System.out.println("Checking "+inputFile);
            File outputFile = new File(inputFile.getParentFile(), outputFilename);
            if (!outputFile.exists() 
                    || outputFile.lastModified() < inputFile.lastModified()) {
                Runnable t = new HTMLTask(inputFile, outputFile);
                if (threaded){
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
                log.error("Interuppted awaiting thread pool termination");
                e.printStackTrace();
            }
        }
        log.info("Finished processing");
    }

}
