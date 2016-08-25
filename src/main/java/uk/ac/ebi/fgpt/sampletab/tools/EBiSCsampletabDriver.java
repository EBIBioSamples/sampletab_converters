package uk.ac.ebi.fgpt.sampletab.tools;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.mged.magetab.error.ErrorItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;
import uk.ac.ebi.arrayexpress2.magetab.datamodel.graph.Node;
import uk.ac.ebi.arrayexpress2.magetab.exception.ParseException;
import uk.ac.ebi.arrayexpress2.magetab.listener.ErrorItemListener;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.GroupNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SCDNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabParser;
import uk.ac.ebi.arrayexpress2.sampletab.parser.SampleTabSaferParser;
import uk.ac.ebi.arrayexpress2.sampletab.renderer.SampleTabWriter;
import uk.ac.ebi.fgpt.sampletab.AbstractDriver;
import uk.ac.ebi.fgpt.sampletab.utils.SampleTabUtils;

public class EBiSCsampletabDriver extends AbstractDriver {

	private Logger log = LoggerFactory.getLogger(getClass());

    private SampleTabSaferParser parser = new SampleTabSaferParser();
	
	public static void main(String[] args){
		new EBiSCsampletabDriver().doMain(args);
	}
	
	@Override
	public void doMain(String[] args) {
		super.doMain(args);
		
		//load the ebisc data
		Path lineFile = Paths.get("/home/faulcon/work/workspace/sampletab_converters/output.csv");
		List<String[]> content = null;

		try (CSVReader reader = new CSVReader(Files.newBufferedReader(lineFile))) {
			// these are small enough to read all into memory at
			// once
			content = reader.readAll();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		log.info("Read input from "+lineFile);
		
		//for each sampletab file containing vials
		//load it
		//check their name is what it should be
		Map<String, String> vialAccToName = new HashMap<>();
		Map<String, Set<String>> subToVialAccs = new HashMap<>();
		for (String[] line : content.subList(1, content.size())) {
			String vialAcc = line[0];
			String vialNameWanted = line[2];
			String sub = line[3];
					
			if (sub.length() > 0 && vialNameWanted.length() > 0) {
				if (!subToVialAccs.containsKey(sub)) {
					subToVialAccs.put(sub, new HashSet<>());
				}
				subToVialAccs.get(sub).add(vialAcc);
				log.info("For submission "+sub+" found vial "+vialAcc);
				
				vialAccToName.put(vialAcc, vialNameWanted);
			}
		}		
		for (String sub : subToVialAccs.keySet()) {
			File origin = new File("/home/faulcon/Desktop/ebisc");
			File file = new File(origin, SampleTabUtils.getSubmissionDirFile(sub).toString());
			File sampleTab = new File(file, "sampletab.txt");
			
			if (sampleTab.exists()) {

				boolean changed = false;
		        SampleData sd;
				try {
					sd = parser.parse(sampleTab);
				} catch (ParseException e1) {
					throw new RuntimeException(e1);
				}
				
				log.info("Read "+sampleTab+" for "+subToVialAccs.get(sub).size()+" vials");
				
				
				for (String vialAcc : subToVialAccs.get(sub)){
					SampleNode sampleNode = null;
					for (SampleNode sample : sd.scd.getNodes(SampleNode.class)) {
						if (vialAcc.equals(sample.getSampleAccession())) {
							sampleNode = sample;
						}
					}
					if (sampleNode != null) {
						if (!vialAccToName.get(vialAcc).equals(sampleNode.getNodeName())) {
							sampleNode.setNodeName(vialAccToName.get(vialAcc));
							log.info("Setting name of "+vialAcc+" to "+sampleNode.getNodeName());
							changed = true;
						}
					} else {
						log.warn("Unable to find accession "+vialAcc+" in submission "+sub);
					}
				}
				if (changed) {
					try (SampleTabWriter sampleTabWriter = new SampleTabWriter(new FileWriter(sampleTab))) {
						sampleTabWriter.write(sd);
						log.info("Wrote to "+sampleTab);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}	
			}
		}
		
		//for each sampletab file containing batchs
		//load it
		//check their name is what it should be
		//check that everything in the batch is a vial
		Map<String, String> batchAccToName = new HashMap<>();
		Map<String, Set<String>> subToBatchAccs = new HashMap<>();
		for (String[] line : content.subList(1, content.size())) {
			String batchAcc = line[12];
			String batchNameWanted = line[14];
			String sub = line[15];
					
			if (sub.length() > 0 && batchNameWanted.length() > 0) {
				if (!subToBatchAccs.containsValue(sub)) {
					subToBatchAccs.put(sub, new HashSet<>());
				}
				subToBatchAccs.get(sub).add(batchAcc);
				
				batchAccToName.put(batchAcc, batchNameWanted);
			}
		}
		
		
		for (String sub : subToBatchAccs.keySet()) {
			File origin = new File("/home/faulcon/Desktop/ebisc");
			File file = new File(origin, SampleTabUtils.getSubmissionDirFile(sub).toString());
			File sampleTab = new File(file, "sampletab.txt");
			
			if (sampleTab.exists()) {

				boolean changed = false;
		        SampleData sd;
				try {
					sd = parser.parse(sampleTab);
				} catch (ParseException e1) {
					throw new RuntimeException(e1);
				}
				
				log.info("Read "+sampleTab);
				
				
				for (String batchAcc : subToBatchAccs.get(sub)){
					GroupNode groupNode = null;
					for (GroupNode batch : sd.scd.getNodes(GroupNode.class)) {
						if (batchAcc.equals(batch.getGroupAccession())) {
							groupNode = batch;
						}
					}
					if (groupNode != null) {
						//change the name
						if (!batchAccToName.get(batchAcc).equals(groupNode.getNodeName())) {
							groupNode.setNodeName(batchAccToName.get(batchAcc));
							log.info("Setting name of "+batchAcc+" to "+groupNode.getNodeName());
							changed = true;
						}
						//check the membership
						Set<Node> toRemove = new HashSet<>();
						
						for (Node parentNode : groupNode.getParentNodes()) {
							if (SampleNode.class.isInstance(parentNode)) {
								if (!parentNode.getNodeName().contains(" vial ")) {
									toRemove.add(parentNode);
								}	
							}
						}
						for (Node toRemoveNode : toRemove) {
							log.info("Removing node "+toRemoveNode.getNodeName()+" from group "+groupNode.getNodeName());
							groupNode.removeParentNode(toRemoveNode);
							toRemoveNode.removeChildNode(groupNode);
							try {
								sd.scd.resolveGraphStructure((SCDNode) toRemoveNode);
							} catch (ParseException e) {
								throw new RuntimeException(e);
							}
							changed = true;
						}
					} else {
						log.warn("Unable to find accession "+batchAcc+" in submission "+sub);
					}
				}
				if (changed) {
					try (SampleTabWriter sampleTabWriter = new SampleTabWriter(new FileWriter(sampleTab))) {
						sampleTabWriter.write(sd);
						log.info("Wrote to "+sampleTab);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}	
			}
		}
		
	}
}
