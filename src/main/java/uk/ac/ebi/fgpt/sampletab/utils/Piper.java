package uk.ac.ebi.fgpt.sampletab.utils;

import java.io.InputStream;
import java.io.OutputStream;

// taken from http://blog.bensmann.com/piping-between-processes

public class Piper implements Runnable {

    private InputStream input;

    private OutputStream output;

    public Piper(InputStream input, OutputStream output) {
        this.input = input;
        this.output = output;
    }

    public static InputStream pipe(Process... proc) throws InterruptedException {
        // Start Piper between all processes
        Process p1;
        Process p2;
        for (int i = 0; i < proc.length; i++) {
            p1 = proc[i];
            // If there's one more process
            if (i + 1 < proc.length) {
                p2 = proc[i + 1];
                // Start piper
                new Thread(new Piper(p1.getInputStream(), p2.getOutputStream())).start();
            }
        }
        java.lang.Process last = proc[proc.length - 1];
        // Wait for last process in chain; may throw InterruptedException
        last.waitFor();
        // Return its InputStream
        return last.getInputStream();
    }

    public static InputStream pipe(InputStream input, Process... proc) throws InterruptedException {
        // Start Piper between all processes
        Process p1;
        Process p2;
        for (int i = 0; i < proc.length; i++) {
            p1 = proc[i];
            if (i == 0 && input != null) {
                new Thread(new Piper(input, p1.getOutputStream())).start();
            }
            // If there's one more process
            if (i + 1 < proc.length) {
                p2 = proc[i + 1];
                // Start piper
                new Thread(new Piper(p1.getInputStream(), p2.getOutputStream())).start();
            }
        }
        java.lang.Process last = proc[proc.length - 1];
        // Wait for last process in chain; may throw InterruptedException
        last.waitFor();
        // Return its InputStream
        return last.getInputStream();
    }

    public void run() {
        try {
            int read = 1;
            // As long as data is read; -1 means EOF
            while (read > -1) {
                // Read bytes into buffer
                read = input.read();
                // System.out.println("read: " + new String(b));
                if (read > -1) {
                    // Write bytes to output
                    output.write(read);
                }
            }
        } catch (Exception e) {
            // Something happened while reading or writing streams; pipe is broken
            throw new RuntimeException("Broken pipe", e);
        } finally {
            try {
                input.close();
            } catch (Exception e) {
                //do nothing
            }
            try {
                output.close();
            } catch (Exception e) {
                //do nothing
            }
            System.out.println("Terminating pipe");
        }
    }

}