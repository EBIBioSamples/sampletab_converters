package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PRIDEutils {

    public static Set<String> getProjects(File xmlFile) throws DocumentException {
        Set<String> projects = new HashSet<String>();
        Document xmldoc = null;
        xmldoc = XMLUtils.getDocument(xmlFile);
        // find the relevant bit in the xml file
        Element expcoll = xmldoc.getRootElement();
        Element exp = XMLUtils.getChildByName(expcoll, "Experiment");
        Element addition = XMLUtils.getChildByName(exp, "additional");
        for (Element cvparam : XMLUtils.getChildrenByName(addition, "cvParam")) {
            if (cvparam.attributeValue("name").equals("Project")) {
                String project = cvparam.attributeValue("value").trim();
                if (project != null && !project.equals("")){
                    projects.add(project);
                }
            }
        }
        return projects;
    }
    
    public static Map<String, Set<String>> loadProjects(File projectsfile) throws IOException{
        Map<String, Set<String>> projects = new HashMap<String, Set<String>>();
        projects = Collections.synchronizedMap(projects);
        
        BufferedReader r = new BufferedReader(new FileReader(projectsfile));
        String line;
        while ((line = r.readLine()) != null){
            String[] parts = line.split("\t");
            String project = parts[0];
            projects.put(project, new HashSet<String>());
            for (int i = 1 ; i < parts.length; i++){
                projects.get(project).add(parts[i]);
            }
        }
        r.close();
        return projects;
    }
    
    public static String[] splitName(String name){
        //strip titles
        name = name.replace("Prof.", "");
        name = name.replace("Dr.", "");
        name = name.replace("Mr.", "");
        name = name.replace("Ms.", "");
        name = name.replace("Mrs.", "");
        name = name.replace("Miss.", "");
        name = name.replace("Sir.", "");
        //any other titles we want to remove?
        
        String firstname = null;
        String initial = null;
        String lastname = null;
        //if there is at least 2 spaces, the first is firstname, the last is surname, and the rest is mid
        //if there is exactly 1 space, first is first, last is surname
        //if there are no spaces, all surname
        if (StringUtils.countMatches(name, " ") == 0){
            firstname = "";
            initial = "";
            lastname = name;
        } else if (StringUtils.countMatches(name, " ") == 1) {
            //TODO if the firstname is only one char, then its an initial instead
            String[] split = name.split(" ");
            firstname = split[0];
            initial = "";
            lastname = split[1];
        } else if (StringUtils.countMatches(name, " ") >= 2) {
            String[] split = name.split(" ");
            firstname = split[0];
            initial = "";
            for (int i = 1; i < split.length-1; i++){
                if (i != 1 )
                    initial = initial + " ";
                initial = initial + split[i];
            }
            lastname = split[split.length-1];
            
        }
        String[] toreturn = new String[3];
        toreturn[0] = firstname.trim();
        toreturn[1] = initial.trim();
        toreturn[2] = lastname.trim();
        return toreturn;
    }
}