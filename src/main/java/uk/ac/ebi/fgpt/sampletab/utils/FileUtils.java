package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.Collection;

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
        }

        public boolean accept(File pathname) {
            if (pathname.getPath().matches(this.regex)) {
                return true;
            } else {
                return false;
            }
        }

    }

    public static void addMatches(File start, FileFilter filter, Collection<File> outfiles) {
        if (start.isDirectory()) {
            File[] subfiles = start.listFiles();
            Arrays.sort(subfiles);
            for (File subfile : subfiles) {
                // System.out.println("Looking in " + subfile);
                addMatches(subfile, filter, outfiles);
            }
        } else {
            if (filter.accept(start)) {
                // System.out.println("Adding " + start);
                outfiles.add(start);
            }
        }
    }
}
