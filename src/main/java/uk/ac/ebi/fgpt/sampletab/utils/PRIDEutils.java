package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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
                String project = cvparam.attributeValue("value");
                projects.add(project);
            }
        }
        return projects;
    }
}