package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public class AgeUtils {
    private final String scriptDirFilename;
    
    private final String ageusername;
    private final String agepassword;
    private final String agehostname;
    private URI ageHostURI;

    private Logger log = LoggerFactory.getLogger(getClass());

    public AgeUtils(String scriptDirFilename, String ageusername, String agepassword, String agehostname){
        this.ageusername = ageusername;
        this.agepassword = agepassword;
        this.agehostname = agehostname;

        URI tempURI = null;
        try {
            tempURI = new URI(agehostname);
        } catch (URISyntaxException e) {
            log.error("Invalid URI "+agehostname, e);
        }
        this.ageHostURI = tempURI;

        this.scriptDirFilename = scriptDirFilename;
    }
    
    public Map<String, Set<String>> BulkTagQuery (Collection<String> submissions){
        
        
        if (submissions.size() > 100){
            //split it into multiples and the recombine
            List<String> subsList = new ArrayList<String>(submissions.size());
            subsList.addAll(submissions);
            
            Map<String, Set<String>> combinedTags = new HashMap<String, Set<String>>();
            
            for (int i=0; i < submissions.size() ; i += 100){
                int maxi = i+100;
                if (maxi > submissions.size()){
                    maxi = submissions.size();
                }
                List<String> partSubmissions = subsList.subList(i, maxi);
                combinedTags.putAll(this.BulkTagQuery(partSubmissions));                
            }
            
            return combinedTags;
            
        } else {
            
            File scriptDir = new File(scriptDirFilename);
            File scriptFile = new File(scriptDir, "TagControl.sh");
            
            String submissionstring = Joiner.on(',').join(submissions);
            
            String command = scriptFile.getAbsolutePath() 
                + " -u "+ageusername
                + " -p "+agepassword
                + " -h \""+agehostname+"\""
                + " -l -sbm "+submissionstring;
    
            ArrayList<String> bashcommand = new ArrayList<String>();
            bashcommand.add("/bin/bash");
            bashcommand.add("-c");
            bashcommand.add(command);
    
            ProcessBuilder pb = new ProcessBuilder();
            pb.command(bashcommand);
            pb.redirectErrorStream(true);// merge stderr to stdout
            Process p = null;
            Map<String, Set<String>> tags = new HashMap<String, Set<String>>();
            try {
                p = pb.start();
                InputStream is = p.getInputStream();
                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                String line;
                while ((line = br.readLine()) != null) {
                    //fast skip blank lines
                    if (line.trim().length() == 0){
                        continue;
                    }
                    
                    //DEBUG
                    log.info(line);
                    String[] parts = line.split(" ");
                    if (parts.length == 2){
                        String subid = parts[1].replace(":", "");
                        String tag = parts[2];
                        if (!tags.containsKey(subid)){
                            tags.put(subid, new HashSet<String>());
                        }
                        tags.get(subid).add(tag);
                    }
                }
                synchronized (p) {
                    p.waitFor();
                }
            } catch (IOException e) {
                log.error("Error running " + command, e);
                return null;
            } catch (InterruptedException e) {
                log.error("Error running " + command, e);
                return null;
            }
            
            return tags;
        }
    }
}
