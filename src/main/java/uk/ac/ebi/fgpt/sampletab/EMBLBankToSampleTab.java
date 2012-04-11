package uk.ac.ebi.fgpt.sampletab;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

public class EMBLBankToSampleTab {

    @Option(name = "-h", usage = "display help")
    private boolean help;

    @Option(name = "-i", usage = "input filename")
    private String inputFilename;

    @Option(name = "-o", usage = "output directory")
    private String outputFilename;

    private Logger log = LoggerFactory.getLogger(getClass());

    
    private static final String SAMPLE_ID = "SAMPLE_ID";
    private static final String ACCESSION = "ACCESSION";
    
    private List<String> splitLine(String line){
        List<String> splitted = new ArrayList<String>();
        return splitted;
    }
    
    private String clean(String in){
        log.info(in);
        in = in.trim();
        if ((in.charAt(0) == '\"') && (in.charAt(in.length()-1) == '\"')){
            in = in.substring(1, in.length()-1);
        }
        log.info(in);
        return in;
    }
    
    private Map<String, Map<String, String>> parseInput(File inputFile) throws MalformedURLException, IOException{
        Map<String, Map<String, String>> data = new HashMap<String, Map<String, String>>();
        
        List<String> headers = null;

        CSVReader reader = null;
        
        try {
            reader = new CSVReader(new FileReader(inputFile));
            String [] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                String accession = null;
                
                if (headers == null){
                    headers = new ArrayList<String>();
                    for (String header : nextLine){
                        headers.add(header);
                    }
                } else {
                    for (int i = 0; i < nextLine.length; i++){
                        String header = headers.get(i);
                        String value = nextLine[i];
                            if (header.equals(ACCESSION)){
                                accession = value;
                            } else if (accession != null){
                                if (!data.containsKey(accession)){
                                    data.put(accession, new HashMap<String, String>());
                                }
                                if (value != null && value.length() > 0){
                                    data.get(accession).put(header, value);
                                }
                            }
                    }
                }
            }
        } finally {
            try {
                if (reader != null){
                    reader.close();
                }
            } catch (IOException e){
                //do nothing
            }
        }
        

        
        return data;
    }

    public static void main(String[] args) {
        new EMBLBankToSampleTab().doMain(args);
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

        File inputFile = new File(inputFilename);
        if (!inputFile.exists()){
            log.error("Unable to load "+inputFilename);
            System.exit(2);
            return;
        }
        
        Map<String, Map<String, String>> data = null;
        try {
            data = parseInput(inputFile);
        } catch (MalformedURLException e) {
            e.printStackTrace();
            System.exit(10);
            return;
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(10);
            return;
        }
        
        log.info("Read "+data.size()+" accessions.");
        
        List<String> accessions = new ArrayList<String>(data.keySet());
        Collections.sort(accessions);
        
        for (String accession : new ArrayList<String>(accessions)) {
            //remove those that already have a sample id
            if (data.get(accession).containsKey(SAMPLE_ID)){
                log.info("Removing "+accession+" because it has a sample ID ("+data.get(accession).get(SAMPLE_ID)+")");
                accessions.remove(accession);
            }
        }
        
        log.info(accessions.size()+" accessions remain.");
        
        Map<Map<String, String>, List<String>> groupings = new HashMap<Map<String, String>, List<String>>();
        
        Set<String> permittedHeaders = new HashSet<String>();
        permittedHeaders.add("ORGANISM");
        permittedHeaders.add("CULTIVAR");
        permittedHeaders.add("ECOTYPE");
        permittedHeaders.add("HOST");
        permittedHeaders.add("ISOLATE");
        permittedHeaders.add("SEROTYPE");
        permittedHeaders.add("SEROVAR");
        permittedHeaders.add("STRAIN");
        permittedHeaders.add("SUB_SPECIES");
        permittedHeaders.add("SUB_STRAIN");
        permittedHeaders.add("VARIETY");
        permittedHeaders.add("TISSUE_TYPE");
        //not ORGANELLE
        //not PLASMID
        
        for (String accession : new ArrayList<String>(accessions)) {
            Map<String, String> identifiers = new HashMap<String, String>();
            for (String key : data.get(accession).keySet()){
                if (permittedHeaders.contains(key)){
                    identifiers.put(key, data.get(accession).get(key));
                }
            }
            if (!groupings.containsKey(identifiers)){
                groupings.put(identifiers, new ArrayList<String>());
            } else if (!data.get(accession).keySet().contains("ENVIRONMENTAL_SAMPLE")) {
                String minAccession = Collections.min(groupings.get(identifiers));
                log.info("Removing "+accession+" because it is the same sample as "+minAccession);
                accessions.remove(accession);
            }
            groupings.get(identifiers).add(accession);
        }
        
        log.info(accessions.size()+" accessions remain.");
    }
}
