package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils {
    // static logger must have name hand-written
    private static Logger log = LoggerFactory.getLogger("uk.ac.ebi.fgpt.sampletab.utils.FileUtils");

    // TODO javadoc
    public static List<File> getParentFiles(File start) {
        List<File> parents = new ArrayList<File>();
        parents.add(new File(start.getName()));
        File totest = start;
        while (totest.getParentFile() != null) {
            totest = totest.getParentFile();
            parents.add(0, new File(totest.getName()));
        }
        return parents;
    }

    // TODO javadoc
    public static File joinFileList(List<File> files) {
        File out = files.get(0);
        int i = 1;
        while (i < files.size()) {
            out = new File(out, files.get(i).getName());
            i++;
        }
        return out;
    }

    // TODO javadoc
    public static String globToRegex(String glob) {
        StringBuilder sb = new StringBuilder(glob.length());
        for (char currentChar : glob.toCharArray()) {
            switch (currentChar) {
                case '*':
                    sb.append(".*");
                    break;
                case '.':
                    sb.append("\\.");
                    break;
                case '?':
                    sb.append("\\?");
                    break;
                default:
                    sb.append(currentChar);
                    break;
            }
        }
        return sb.toString();
    }

    // TODO javadoc
    public static List<File> getMatchesGlob(String glob) {
        log.debug("glob = " + glob);
        return getMatchesRegex(globToRegex(glob));
    }

    // TODO javadoc
    public static List<File> getMatchesRegex(String regex) {
        log.debug("regex = " + regex);
        List<File> outfiles = new ArrayList<File>();
        File regfile = new File(regex);
        log.debug("regfile = " + regfile);

        List<File> regparents = getParentFiles(regfile);
        log.debug("regparents = " + regparents);
        int i;
        for (i = 0; i < regparents.size(); i++) {
            if (regparents.get(i).getName().contains(".*") || regparents.get(i).getName().contains("?")) {
                break;
            }
        }
        if (i == regparents.size()){
            //no things left
            //therefore only one match
            String filename = regex.replace("\\.", ".");
            outfiles.add(new File(filename));
        } else {
            regex = joinFileList(regparents.subList(i, regparents.size())).toString();
            log.debug("cleaned regex : " + regex);
            File start = joinFileList(regparents.subList(0, i));
            log.debug("cleaned start : " + start);
            addMatches(start, regex, outfiles, 0);
        }
        Collections.sort(outfiles);
        return outfiles;
    }

    // TODO javadoc
    private static void addMatches(File file, String regex, Collection<File> outfiles, int depth) {
        log.debug("checking " + file);
        if (file.isDirectory()) {
            File[] subfiles = file.listFiles();
            Arrays.sort(subfiles);
            for (File subfile : subfiles) {
                addMatches(subfile, regex, outfiles, depth + 1);
            }
        } else {
            if (file.getPath().matches(regex)) {
                outfiles.add(file);
            }
        }
    }
}
