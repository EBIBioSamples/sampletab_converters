package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessUtils {
    private static Logger log = LoggerFactory.getLogger("uk.ac.ebi.fgpt.sampletab.utils.ProcessUtils");
    
    public static boolean doCommand(String command, File logfile) {
        log.debug(command);

        ArrayList<String> bashcommand = new ArrayList<String>();
        bashcommand.add("/bin/bash");
        bashcommand.add("-c");

        ProcessBuilder pb = new ProcessBuilder();
        if (logfile != null){
            //this is Java 7 only shorthand
            //pb.redirectOutput(logfile);
            //pb.redirectErrorStream(true);// merge stderr to stdout
            
            //pre-Java 7 we do this using bash
            command = command+" 2>&1 > "+logfile;
            
        }
        bashcommand.add(command);
        pb.command(bashcommand);

        Process p;
        try {
            p = pb.start();
            synchronized (p) {
                p.waitFor();
            }
            if (p.exitValue() != 0) {
                log.error("Error running " + command);
                log.error("Exit code is " + p.exitValue());
                return false;
            }
        } catch (IOException e) {
            log.error("Error running " + command, e);
            return false;
        } catch (InterruptedException e) {
            log.error("Error running " + command, e);
            return false;
        }
        return true;

    }

}
