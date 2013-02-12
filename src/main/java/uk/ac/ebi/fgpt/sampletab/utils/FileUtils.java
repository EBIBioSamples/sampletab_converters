package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

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

    
    public static void copy(File sourceFile, File destFile) throws IOException {
        sourceFile = sourceFile.getAbsoluteFile();
        destFile = destFile.getAbsoluteFile();
        if (!destFile.getParentFile().exists() && !destFile.getParentFile().mkdirs()){
            throw new IOException("Unable to make directories for "+destFile);
        }
        Files.copy(sourceFile, destFile);
    }
    
    public static void move(File sourceFile, File destFile) throws IOException {
        sourceFile = sourceFile.getAbsoluteFile();
        destFile = destFile.getAbsoluteFile();
        if (!destFile.getParentFile().exists() && !destFile.getParentFile().mkdirs()){
            throw new IOException("Unable to make directories for "+destFile);
        }
        Files.move(sourceFile, destFile);
    }

    public static List<File> getMatchesGlob(String string) {
        List<File> filelist = new LinkedList<File>();
        for (File file : new FileGlobIterable(string)){
            filelist.add(file);
        }
        return filelist;
    }

    public static List<File> getRecursiveFiles(String string) {
        List<File> filelist = new LinkedList<File>();
        File startfile =  new File(".");
        startfile = startfile.getAbsoluteFile();
        startfile = startfile.getParentFile();
        log.info("startfile = "+startfile);
        for (File file : new FileRecursiveIterable(string,startfile)){
            filelist.add(file);
        }
        return filelist;
    }
}
