package uk.ac.ebi.fgpt.sampletab.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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
    
    @Option(name = "--help", aliases={"--h"}, usage = "display help")
    protected boolean help;
    
    @Option(name = "--output", aliases={"-o"}, usage = "output filename")
    protected String outputFilename = "zooma.txt";

    @Option(name = "--minimum", aliases={"-m"}, usage = "minimum count")
    protected int minimumcount = 10;

    @Option(name = "--maximum", aliases={"-x"}, usage = "maximum query length")
    protected int maximumlength = 100;
    
    @Option(name = "--standalone", aliases={"-a"}, usage = "output for standalone tool")
    protected boolean standalone;
    
    
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

            if (!standalone) {
                writer.write("Key\tValue\tmatch\tscore\tdirect\tcorrect?\n");
            }
            
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
                            
                            if (count >= minimumcount 
                                    && value.length() <= maximumlength
                                    && !value.matches("[0-9\\-.]+")
                                    && !value.equals("n\\a")
                                    && !value.equals("N\\A")
                                    && !value.equals("none")
                                    && !value.equals("NONE")
                                    && !value.equals("None")) {
                                log.info(key+", "+value+", "+count);
                                

                                if (standalone) {
                                    //output two records for standalone, one with key one without
                                    writer.write(value+"\t"+key+"\n");
                                    writer.write(value+"\t\n");
                                } else {
                                    boolean direct = true;
                                    
                                    int j = 0;
                                    
                                    JsonNode allHits = CorrectorZooma.getAllNodesOfKeyValueQuery(key, value);
                                    if (allHits == null) {
                                        //did not find any hits
                                    } else {
                                        //found at least one hit
                                        int k = 0;
                                        JsonNode hit = allHits.get(k);
                                        while (hit != null && j < 3) {
                                            String fixName = hit.get("name").getTextValue();
                                            String score = hit.get("score").getTextValue();
                                            writer.write(key+"\t"+value+"\t"+fixName+"\t"+score+"\t"+direct+"\n");
                                            
                                            j++;
                                            k++;
                                            hit = allHits.get(k);
                                        }
                                    }
                                    
                                    //fall back to indirect
                                    direct = false;
                                    allHits = CorrectorZooma.getAllNodesOfValueQuery(value);
                                    if (allHits == null) {
                                        //did not find any hits
                                        //end the outer look
                                    } else {
                                        //found at least one hit
                                        int k = 0;
                                        JsonNode hit = allHits.get(k);
                                        while (hit != null && j < 3) {
                                            String fixName = hit.get("name").getTextValue();
                                            String score = hit.get("score").getTextValue();
                                            writer.write(key+"\t"+value+"\t"+fixName+"\t"+score+"\t"+direct+"\n");
                                            
                                            j++;
                                            k++;
                                            hit = allHits.get(k);
                                        }
                                    }
                                    
                                    if (j == 0){
                                        //output at least the key & value once
                                        writer.write(key+"\t"+value+"\n");
                                    }
                                }
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
