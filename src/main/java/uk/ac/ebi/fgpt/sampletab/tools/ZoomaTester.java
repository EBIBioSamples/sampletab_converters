package uk.ac.ebi.fgpt.sampletab.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.codehaus.jackson.JsonNode;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.fgpt.sampletab.zooma.CorrectorZooma;


public class ZoomaTester {

    @Argument(required=true, index=0, metaVar="INPUT", usage = "input filename")
    protected String inputFilename;
    
    @Option(name = "-h", aliases={"--help"}, usage = "display help")
    protected boolean help;
    
    @Option(name = "-o", aliases={"--output"}, usage = "output filename")
    protected String outputFilename = "zooma.txt";

    @Option(name = "-m", aliases={"--minimum"}, usage = "minimum count")
    protected int minimumcount = 10;
    
    private Logger log = LoggerFactory.getLogger(getClass());
    
    protected CmdLineParser cmdParser = new CmdLineParser(this);
    
    private Pattern pattern = Pattern.compile("^(.*?) \\(([0-9]++)\\)$");


    public static void main(String[] args) {
        new ZoomaTester().doMain(args);
    }
    
    protected void doMain(String[] args){
        try {
            // parse the arguments.
            cmdParser.parseArgument(args);
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
        
        File inputFile = new File(inputFilename);
        File outputFile = new File(outputFilename);
        
        
        BufferedReader reader = null;
        BufferedWriter writer = null;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
            writer = new BufferedWriter(new FileWriter(outputFile));

            writer.write("Key\tValue\tmatch\tscore\tdirect\n");
            
            String line = null;  
            while ((line = reader.readLine()) != null){  
                String[] entries = line.split("\t");
                String key = entries[0];
                
                java.util.regex.Matcher m = pattern.matcher(key);
                
                if (!m.matches()){
                    log.error("Matcher did not match: "+key);
                } else {
                    
                    key = m.group(1);
                    
                
                    if (key.startsWith("characteristic[") && key.endsWith("]")){
                        key = key.replace("characteristic[", "");
                        key = key.substring(0, key.length()-1);
                    } else if (key.startsWith("comment[") && key.endsWith("]")){
                        key = key.replace("comment[", "");
                        key = key.substring(0, key.length()-1);
                    }
    
                    
                    for(int i = 1; i < entries.length; i++){
                        String entry = entries[i];
                        m = pattern.matcher(entry);
                        if (!m.matches()){
                            log.error("Matcher did not match: "+entry);
                        } else {
                            String value = m.group(1);
                            
                            Integer count = new Integer(m.group(2));
                            if (count >= minimumcount){
                            
                                //log.info(key+"\t"+value+"\t"+count);
                                
                                Boolean direct = null;
                                String fixName = null;
                                Float score = null;
                                
                                JsonNode tophit = null;
                                try{
                                    tophit = CorrectorZooma.getZoomaKeyValueHit(key, value);
                                } catch (IOException e) {
                                    if (e.getMessage().contains("Server returned HTTP response code: 500 for URL")){
                                        //do nothing
                                    } else {
                                        throw e;
                                    }
                                }
                                if (tophit != null){
                                    direct = true;
                                    fixName = tophit.get("name").getTextValue();
                                    score = new Float(tophit.get("score").getTextValue());
                                } else {
                                    //retry without key
                                    try{
                                        tophit = CorrectorZooma.getZoomaStringHit(value);
                                    } catch (IOException e) {
                                        if (e.getMessage().contains("Server returned HTTP response code: 500 for URL")){
                                            //do nothing
                                        } else {
                                            throw e;
                                        }
                                    }
                                    if (tophit != null){
                                        direct = false;
                                        fixName = tophit.get("name").getTextValue();
                                        score = new Float(tophit.get("score").getTextValue());
                                    }
                                }
                                writer.write(key+"\t"+value);
                                if (fixName != null){
                                    writer.write("\t"+fixName+"\t"+score+"\t"+direct);
                                }
                                writer.write("\n");
                            }
                        }
                    }
                }
            }
        } catch (FileNotFoundException e) {
            log.error("Unable to find "+inputFile, e);
        } catch (IOException e) {
            log.error("Unable to read "+inputFile, e);
        } catch (Throwable e) {
            log.error("Problem with "+inputFile, e);
        } finally {
            if (writer != null){
                try {
                    writer.close();
                } catch (IOException e) {
                    // do nothing
                }
            }
        }
    }
}
