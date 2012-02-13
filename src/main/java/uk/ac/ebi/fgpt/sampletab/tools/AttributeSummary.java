package uk.ac.ebi.fgpt.sampletab.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SCDNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabParser;
import uk.ac.ebi.fgpt.sampletab.SampleTabToLoad;
import uk.ac.ebi.fgpt.sampletab.utils.FileUtils;

public class AttributeSummary {

    @Option(name = "-h", usage = "display help")
    private boolean help;

    @Option(name = "-i", aliases={"--input"}, usage = "input filename or glob")
    private String inputFilename;
    
    @Option(name = "-o", aliases={"--output"}, usage = "output filename")
    private String outputFilename;

    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;

    @Option(name = "--comments", usage = "output comments rather than characteristics?")
    private boolean docomments = false;

	// logging
	private Logger log = LoggerFactory.getLogger(getClass());
	
	private volatile Map<String, Map<String, Integer>> comments = Collections
			.synchronizedMap(new HashMap<String, Map<String, Integer>>());
	private volatile Map<String, Map<String, Integer>> characteristics = Collections
			.synchronizedMap(new HashMap<String, Map<String, Integer>>());
	

	
	private class ProcessTask implements Runnable {
		private File inFile;
		ProcessTask(File inFile){
			this.inFile = inFile;
		}
		@Override
		public void run() {
			SampleTabParser<SampleData> parser = new SampleTabParser<SampleData>();
			SampleData sampledata;
			try {
				sampledata = parser.parse(this.inFile);
			} catch (ParseException e) {
				log.error("Unable to parse "+inFile);
				e.printStackTrace();
				return;
			}
			
			for (SCDNode node : sampledata.scd.getAllNodes()) {
				for (SCDNodeAttribute attribute : node.getAttributes()) {
					Map<String, Map<String, Integer>> map = null;
					if (CommentAttribute.class.isInstance(attribute)) {
						map = comments;
					} else if (CharacteristicAttribute.class.isInstance(attribute)) {
						map = characteristics;
					}
					if (map != null) {
						String key = attribute.getAttributeType();
						String value = attribute.getAttributeValue();
						if (!map.containsKey(key)) {
							map.put(key,
									Collections
											.synchronizedMap(new HashMap<String, Integer>()));
						}
						if (map.get(key).containsKey(value)) {
							int count = map.get(key).get(value).intValue();
							map.get(key).put(value,
									count + 1);
						} else {
							map.get(key).put(value, new Integer(1));
						}
					}
				}
			}
		}
		
	}
    
	class KeyComparator implements Comparator {
		private final Map<String, Map<String, Integer>> map;
		
		public KeyComparator(Map<String, Map<String, Integer>> map){
			super();
			this.map = map;
		}
		
		@Override
		public int compare(Object arg0, Object arg1) {
			int firstval = 0;
			for (Integer value : this.map.get(arg0).values()){
				firstval += value;
			}
			
			int secondval = 0;
			for (Integer value : this.map.get(arg1).values()){
				secondval += value;
			}
			
			if (firstval < secondval) {
				return -1;
			} else if (firstval > secondval){
				return 1;
			} else {
				return 0;
			}
		}
	}
	
	class ValueComparator implements Comparator {
		private final Map<String, Integer> map;
		
		public ValueComparator(Map<String, Integer> map){
			super();
			this.map = map;
		}
		
		@Override
		public int compare(Object arg0, Object arg1) {
			int firstval = this.map.get(arg0);
			int secondval = this.map.get(arg1);
			
			if (firstval < secondval) {
				return -1;
			} else if (firstval > secondval){
				return 1;
			} else {
				return 0;
			}
		}
	}

	public static void main(String[] args) {
        new AttributeSummary().doMain(args);
    }

    public void doMain(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            // parse the arguments.
            parser.parseArgument(args);
            // TODO check for extra arguments?
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            help = true;
        }

        if (help) {
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
            return;
        }

        List<File> inputFiles = new ArrayList<File>();
        inputFiles = FileUtils.getMatchesGlob(inputFilename);
        log.info("Found " + inputFiles.size() + " input files");
        Collections.sort(inputFiles);

        int nothreads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(nothreads);

        for (File inputFile : inputFiles) {
            Runnable t = new ProcessTask(inputFile);
            if (threaded) {
                pool.execute(t);
            } else {
                t.run();
            }
        }
        
        // run the pool and then close it afterwards
        // must synchronize on the pool object
        synchronized (pool) {
            pool.shutdown();
            try {
                // allow 24h to execute. Rather too much, but meh
                pool.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                log.error("Interupted awaiting thread pool termination");
                e.printStackTrace();
            }
        }
        log.info("Finished reading");
        
        //now to write back out
        Writer out = null;
    	
    	Map<String, Map<String, Integer>> map;
    	if (docomments){
    		map = comments;
    	} else {
    		map = characteristics;
    	}
    	
    	ArrayList<String> keys = new ArrayList<String>(map.keySet());
    	Collections.sort(keys, new KeyComparator(map));
        
        try {
        	out = new BufferedWriter(new FileWriter(new File(outputFilename)));
        	
        	
        	for (String key : keys){
				int total = 0;
				for (Integer value : map.get(key).values()){
					total += value;
				}
        		out.write(key+" ("+total+")\t");
        		
        		ArrayList<String> values = new ArrayList<String>(map.get(key).keySet());
        		Collections.sort(values, new ValueComparator(map.get(key)));
        		
        		for (String value : values){
        			out.write(value+" ("+map.get(key).get(value)+")\t");
        		}
        		
        		out.write("\n");
        	}
        	
        } catch (IOException e) {
			log.error("Unable to output to "+outputFilename);
			e.printStackTrace();
		} finally {
        	if (out != null) {
        		try {
        			out.close();
        		} catch (IOException e){
        			//do nothing
        		}
        	}
        }
        
	}

}
