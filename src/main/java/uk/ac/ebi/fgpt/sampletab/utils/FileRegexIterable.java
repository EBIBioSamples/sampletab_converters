package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FileRegexIterable implements Iterable<File> {

    private final String regex;
    
    public FileRegexIterable(String regex){
        this.regex = regex;
    }

    @Override
    public Iterator<File> iterator() {
        return new FileRegexIterator(regex);
    }
    
    private class FileRegexIterator implements Iterator<File> {
    
        private final BlockingQueue<File> filequeue = new LinkedBlockingQueue<File>();
        private final Thread thread;
        
        private final Logger log = LoggerFactory.getLogger(getClass());
    
        
        public FileRegexIterator(String regex){
            thread = new Thread(new FileRegexRunnable(regex, filequeue));
            thread.start();
        }
        @Override
        public boolean hasNext() {
            while (thread.isAlive()) {
                
                if (!filequeue.isEmpty()) {
                    return true;
                }
                
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    log.error("Interrupted waiting for queue to be non-empty");
                    thread.interrupt();
                    return false;
                }
            }
            if (filequeue.isEmpty()) {
                //can only guarantee no more files when thread is dead and list is empty
                return false;
            } else {
                return true;
            }
        }
    
        @Override
        public File next() {
            try {
                return filequeue.take();
            } catch (InterruptedException e) {
                log.error("Interupted taking item from queue", e);
                return null;
            }
        }
    
        @Override
        public void remove() {
            throw new RuntimeException("GlobIterator.remove() not implemented");
        }
        
        private class FileRegexRunnable implements Runnable {
            private final BlockingQueue<File> filequeue;
            private final String regex;
            
            private final Logger log = LoggerFactory.getLogger(getClass());
    
            public FileRegexRunnable(String regex, BlockingQueue<File> filequeue){
                this.regex = regex;
                this.filequeue = filequeue;
            }
    
            @Override
            public void run() {
                try {
                    addMatchesRegex(null);
                } catch (InterruptedException e) {
                    log.error("Interrupted while matching "+regex, e);
                } 
            }
            
            public void addMatchesRegex(File startfile) throws InterruptedException {
                log.debug("regex = " + regex);
                
                File regfile;
                if (startfile == null){
                    regfile = new File(regex);
                } else {
                    regfile = new File(startfile, regex);
                }
                regfile = regfile.getAbsoluteFile();
                List<String> regparts = new ArrayList<String>();
                regparts.add(regfile.getName());
                File totest = regfile;
                while (totest.getParentFile() != null) {
                    totest = totest.getParentFile();
                    regparts.add(0, totest.getName());
                }
                //trim root
                if (regparts.get(0).equals("")){
                    regparts = regparts.subList(1, regparts.size());
                }
                
                log.debug("regparts= " + regparts);
                
                File root = regfile;
                while (root.getParentFile() != null){
                    root = root.getParentFile();
                }
                File[] subfiles = root.listFiles();
                Arrays.sort(subfiles);
                for (File subfile : subfiles) {
                    lookAt(subfile, regparts);
                }
            }
    
            private void lookAt(File tolook, List<String> regparts) throws InterruptedException{
                log.debug("tolook : "+tolook);
                log.debug("regparts.get(0) : "+regparts.get(0));
                log.debug("matches : "+tolook.getName().matches(regparts.get(0)));
                if (tolook.getName().matches(regparts.get(0))){
                    if (regparts.size() == 1){
                        filequeue.put(tolook);
                    } else {
                        log.debug("isDirectory() : "+tolook.isDirectory());
                        log.debug("listFiles() : "+tolook.listFiles());
                        if (tolook.isDirectory()) {
                            File[] subfiles = tolook.listFiles();
                            Arrays.sort(subfiles);
                            for (File subfile : subfiles) {
                                lookAt(subfile, regparts.subList(1, regparts.size()));
                            }
                        }
                    }
                }
            }
        }
    }
}