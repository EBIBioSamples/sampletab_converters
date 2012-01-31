package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class FileUtils {

    public static class FileFilterRegex implements FileFilter {

        private final String regex;

        public FileFilterRegex(String regex) {
            this.regex = regex;
        }

        public boolean accept(File pathname) {
            if (pathname.getPath().matches(this.regex)) {
                return true;
            } else {
                return false;
            }
        }

    }

    public static class FileFilterGlob implements FileFilter {

        private final String regex;
        private final File regfile;
        
        public FileFilterGlob(String glob) {
            StringBuilder sb = new StringBuilder(glob.length());
            for (char currentChar : glob.toCharArray()) {
                switch (currentChar) {
                    case '*':
                        sb.append(".*");
                    case '.':
                        sb.append("\\.");
                    case '?':
                        sb.append("\\?");
                }
            }

            this.regex = sb.toString();
            this.regfile = new File(this.regex);
        }

        public boolean accept(File pathname) {
            if (pathname.getPath().matches(this.regex)) {
                return true;
            } else {
                return false;
            }
        }

    }

    public static List<File> getMatches(File start, String regex) {
        List<File> outfiles = new ArrayList<File>();
        File regfile = new File(regex);
        addMatches(start, regex, outfiles, 0);
        return outfiles;
    }
    
    private static void addMatches(File file, String regex, Collection<File> outfiles, int depth) {
        if (file.isDirectory()) {
            File[] subfiles = file.listFiles();
            Arrays.sort(subfiles);
            for (File subfile : subfiles) {
                addMatches(subfile, regex, outfiles, depth+1);
            }
        } else {
            if (file.getPath().matches(regex)) {
                outfiles.add(file);
            }
        }
    }

//    public static List<File> getMatches(File start, FileFilter filter) {
//        List<File> outfiles = new ArrayList<File>();
//        addMatches(start, filter, outfiles, 0);
//        return outfiles;
//    }
//    
//    private static void addMatches(File start, FileFilter filter, Collection<File> outfiles, int depth) {
//        if (start.isDirectory()) {
//            File[] subfiles = start.listFiles();
//            Arrays.sort(subfiles);
//            for (File subfile : subfiles) {
//                addMatches(subfile, filter, outfiles, depth+1);
//            }
//        } else {
//            if (filter.accept(start)) {
//                outfiles.add(start);
//            }
//        }
//    }
}
