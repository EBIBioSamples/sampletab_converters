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
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
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
    
    @Option(name = "-r", aliases={"--rows"}, usage = "number of attributes")
    private int rows = 100;
    
    @Option(name = "-c", aliases={"--columns"}, usage = "number of values of attributes")
    private int cols = 100;
    
    @Option(name = "--organism", aliases={"--organism"}, usage = "organism to use")
    private String organism = null;

    @Option(name = "--threaded", usage = "use multiple threads?")
    private boolean threaded = false;

    @Option(name = "--totals", usage = "display total counts?")
    private boolean totals = false;

	// logging
	private Logger log = LoggerFactory.getLogger(getClass());
	
	protected volatile Map<String, Map<String, Integer>> attributes = Collections
			.synchronizedMap(new HashMap<String, Map<String, Integer>>());
	
	private class ProcessTask implements Runnable {
		private File inFile;
		ProcessTask(File inFile){
			this.inFile = inFile;
		}
		
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
			    if (includeNode(node)) {
    				for (SCDNodeAttribute attribute : node.getAttributes()) {
    				    processAttribute(attribute);
    				}
			    }
			}
		}
		
	}
    
	protected boolean includeNode(SCDNode node){
	    if (organism == null){
	        return true;
	    } else {
	        //get organism of node
	        String nodeorganism = null;
	        for (SCDNodeAttribute attr : node.getAttributes()){
	            if (OrganismAttribute.class.isInstance(attr)){
	                nodeorganism = attr.getAttributeValue();
	            }
	        }
	        //compare to this.organism
	        if (organism.equals(nodeorganism)){
	            return true;
	        }else {
	            return false;
	        }
	    }
	}
	
	protected void processAttribute(SCDNodeAttribute attribute){
        String key = attribute.getAttributeType();
        String value = attribute.getAttributeValue();
        if (!attributes.containsKey(key)) {
            attributes.put(key,
                    Collections
                            .synchronizedMap(new HashMap<String, Integer>()));
        }
        if (attributes.get(key).containsKey(value)) {
            int count = attributes.get(key).get(value).intValue();
            attributes.get(key).put(value,
                    count + 1);
        } else {
            attributes.get(key).put(value, new Integer(1));
        }
	}
	
	private class KeyComparator implements Comparator {
		private final Map<String, Map<String, Integer>> map;
		
		public KeyComparator(Map<String, Map<String, Integer>> map){
			super();
			this.map = map;
		}
		
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
            parser.printSingleLineUsage(System.err);
            System.err.println();
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
            return;
        }

        log.info("Looking for input files "+inputFilename);
        List<File> inputFiles = new ArrayList<File>();
        inputFiles = FileUtils.getMatchesGlob(inputFilename);
        log.info("Found " + inputFiles.size() + " input files");
        Collections.sort(inputFiles);

        ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

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
    	
    	ArrayList<String> keys = new ArrayList<String>(attributes.keySet());
    	Collections.sort(keys, new KeyComparator(attributes));
        Collections.reverse(keys);
        
        try {
        	out = new BufferedWriter(new FileWriter(new File(outputFilename)));
        	
        	int rowcount = 0;
        	for (String key : keys){
				int total = 0;
				for (Integer value : attributes.get(key).values()){
					total += value;
				}
        		out.write(key+" ("+total+")\t");
        		
        		int colcount = 0;
        		ArrayList<String> values = new ArrayList<String>(attributes.get(key).keySet());
        		Collections.sort(values, new ValueComparator(attributes.get(key)));
        		Collections.reverse(values);
        		
        		for (String value : values){
        			out.write(value+" ("+attributes.get(key).get(value)+")\t");
        			colcount += 1;
        			if (this.cols > 0 && colcount > this.cols){
        			    break;
        			}
        		}
        		
        		out.write("\n");
        		rowcount += 1;
                if (this.rows > 0 && rowcount > this.rows){
                    break;
                }
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
        
        if (totals){
            int total = 0;
            for (String key : keys){
                for (Integer value : attributes.get(key).values()){
                    total += value;
                }
            }
            System.out.println("Total number of attributes: "+total);
        }
        
	}

}
