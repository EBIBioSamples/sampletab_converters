package uk.ac.ebi.fgpt.sampletab.tools;

import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.jolbox.bonecp.BoneCPDataSource;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import uk.ac.ebi.fgpt.sampletab.AbstractDriver;
import uk.ac.ebi.fgpt.sampletab.Accessioner;
import uk.ac.ebi.fgpt.sampletab.Corrector;
import uk.ac.ebi.fgpt.sampletab.utils.XMLUtils;

public class EBiSCcsvDriver extends AbstractDriver {

    @Option(name = "--csv", usage = "csv path")
    private String csvPath;
    
    @Option(name = "--csv-extra", usage = "csv path for extra file")
    private String csvExtraPath;
    
	public enum AccessionType {
		BATCH, LINE, DONOR, VIAL, OTHER;
	}

	private Accessioner accessioner;

	private JdbcTemplate relationalTemplate;

	private Logger log = LoggerFactory.getLogger(getClass());

	private Map<String, String> sampleToGroup = new ConcurrentHashMap<>();

	private Map<String, Set<String>> groupToSamples = new ConcurrentHashMap<>();

	private Map<String, String> derivedFrom = new ConcurrentHashMap<>();
	private Map<String, Set<String>> derivedTo = new ConcurrentHashMap<>();

	private Map<String, String> accessionToOwner = new ConcurrentHashMap<>();
	private Map<String, String> accessionToSubmission = new ConcurrentHashMap<>();

	private Map<String, AccessionType> accessionToType = new ConcurrentHashMap<>();

	private Map<String, String> accessionToName = new ConcurrentHashMap<>();

	private Map<String, String> accessionToRC = new ConcurrentHashMap<>();
	
	private Map<String, String> hipsciToEbisc = new ConcurrentHashMap<>();

	private String urlRoot = "https://www.ebi.ac.uk/biosamples/";
	//private String urlRoot = "http://cocoa.ebi.ac.uk:14114/biosamples/";
	
	
	
	public static void main(String[] args) {
		new EBiSCcsvDriver().doMain(args);
	}

	private DataSource getBioSampleDataSource() {
		BoneCPDataSource ds = new BoneCPDataSource();
		ds.setJdbcUrl("jdbc:oracle:thin:@ora-vm-023.ebi.ac.uk:1531:biosdpro");
		ds.setUser("biosd");
		ds.setPassword("b10sdp40");
		return ds;
	}

	private DataSource getAccessionDataSource() {
		BoneCPDataSource ds = new BoneCPDataSource();
		ds.setJdbcUrl("jdbc:oracle:thin:@ora-vm-023.ebi.ac.uk:1531:biosdpro");
		ds.setUser("bsd_acc");
		ds.setPassword("b5d4ccpr0");
		return ds;
	}

	@Override
	public void doMain(String[] args) {
		super.doMain(args);

		accessioner = new Accessioner(getAccessionDataSource());
		relationalTemplate = new JdbcTemplate(getBioSampleDataSource());
		
		try {
			//handleRCData(Paths.get("/home/faulcon/Desktop/ebisc/ebisc-data/outgoing/batch_csv/"));
			// query biosamples api to get all samples matching "ebisc"
			// for each group, get all the samples in it
			// for each sample, get the "derived from" relationships
			// for each sample/group, get who owns the accession
			// for each sample/group, get the submission
			Set<String> groupAccessions = null;
			groupAccessions = getEBiSCGroups();
			
			groupAccessions.parallelStream().forEach((acc)-> safeHandleGroup(acc));

			// TODO check that each group has name "ebisc"
			// TODO check that only vials are in groups

			// TODO check that donors have no derived to
			// TODO check that vials always have derived to
			// TODO check that vials always have a line
			// TODO check that lines always have a donor

			// output
			/// vial vial name vial sub line line sub hESCreg donor donor sub
			// batch

		} catch (DocumentException e) {
			throw new RuntimeException("Problem getting ebisc data", e);
		} catch (MalformedURLException e) {
			throw new RuntimeException("Problem getting ebisc data", e);
		} catch (IOException e) {
			throw new RuntimeException("Problem getting ebisc data", e);
		}

		// handle an extra manual file from RC
		//must be done AFTER biosample XML queries
		try {
			handleExtraFile2();
		} catch (IOException e) {
			throw new RuntimeException("Problem getting ebisc RC batch data", e);
		}
		

		output();
	}
	
	public void handleExtraFile2() throws IOException {

		
		Path lineFile = Paths.get(csvExtraPath);

		try (CSVReader reader = new CSVReader(Files.newBufferedReader(lineFile))) {

			// these are small enough to read all into memory at
			// once
			List<String[]> content = reader.readAll();

			String[] headers = content.get(0);
			
			// need to extract the batch name and the biosample
			// accession of the batch
			int headerBiosamplesCellLineId = -1;
			int headerBiosamplesBatchId = -1;
			int headerBatch = -1;
			int headerDepositorCellLineName = -1;
			int headerHESCregCellLineName = -1;
			for (int i = 0; i < headers.length; i++) {
				if ("BioSamples Cell Line ID".equals(headers[i])) {
					headerBiosamplesCellLineId = i;
				}
				if ("Biosamples Batch ID".equals(headers[i])) {
					headerBiosamplesBatchId = i;
				}
				if ("Batch".equals(headers[i])) {
					headerBatch = i;
				}
				if ("Depositor Cell Line Name".equals(headers[i])) {
					headerDepositorCellLineName = i;
				}
				if ("hESCreg Cell Line Name".equals(headers[i])) {
					headerHESCregCellLineName = i;
				}
			
			}
			
			for (String[] line : content.subList(1, content.size())) {
				String biosamplesCellLineId = line[headerBiosamplesCellLineId].trim();
				String batch = line[headerBatch].trim();
				String biosamplesBatchId = line[headerBiosamplesBatchId].trim();
				String depositorCellLineName = line[headerDepositorCellLineName].trim();
				String hESCregCellLineName = line[headerHESCregCellLineName].trim();
				
				biosamplesCellLineId = Corrector.cleanString(biosamplesCellLineId);
				batch = Corrector.cleanString(batch);
				biosamplesBatchId = Corrector.cleanString(biosamplesBatchId);
				depositorCellLineName = Corrector.cleanString(depositorCellLineName);
				hESCregCellLineName = Corrector.cleanString(hESCregCellLineName);
				
				if (batch.trim().length() > 0 && biosamplesBatchId.trim().length() > 0) {
					if (!accessionToRC.containsKey(biosamplesBatchId)) {
						log.info("Associating batch "+biosamplesBatchId+" with name "+batch);
						accessionToRC.put(biosamplesBatchId, batch);
					}
				}
				
				//try and go from cell line to single batch
				Set<String> vials = getVialsOfLine(biosamplesCellLineId);
				Set<String> batches = new HashSet<>();
				for (String vial : vials) {
					if (!sampleToGroup.containsKey(vial)) {
						log.warn("Vial "+vial+" of line "+biosamplesCellLineId+" is not in any groups");
					} else {
						batches.add(sampleToGroup.get(vial));
					}
				}
				
				if (batches.size()== 1) {
					//ASSUME that the one group in biosample is the one batch in RC
					String groupAcc = batches.iterator().next();
					if (accessionToRC.containsKey(groupAcc)) {
						if (!accessionToRC.get(groupAcc).equals(batch)) {
							log.error("Conflicting batch names for "+groupAcc+" ("+batch+" vs "+accessionToRC.get(groupAcc)+")");
						}
						//otherwise the names match, so no action needed
					} else {
						log.info("Associating batch "+groupAcc+" with name "+batch);
						accessionToRC.put(groupAcc, batch);
					}
				} else {
					log.info("Number of batches for line "+biosamplesCellLineId+" is "+batches.size()+" not 1");
				}
				
				//if (depositorCellLineName.startsWith("HPSI")) {
				if (hESCregCellLineName.startsWith("WTSI")) {
					hipsciToEbisc.put(depositorCellLineName, hESCregCellLineName);
				}
			
			}
		}
	}

	public Set<String> getEBiSCGroups() throws DocumentException, MalformedURLException, IOException {
		log.info("Getting EBiSC groups from BioSamples XML API");
		Set<String> result = new HashSet<>();

		int page = 1;
		Integer to = null;
		Integer total = null;
		while (to == null || to < total) {
			// get an xml for the query
			Document doc = XMLUtils.getDocument(new URL(urlRoot+"xml/group/query=EBiSC&pagesize=500&page=" + page));
			Element root = doc.getRootElement();

			// process this pages results
			for (Element group : XMLUtils.getChildrenByName(root, "BioSampleGroup")) {
				String groupAcc = group.attributeValue("id");
				log.info("Found group " + groupAcc);
/*
				if (groupAcc.equals("SAMEG316153")) {
					log.warn("Skipping " + groupAcc + " as it is waiting for update on 10th June");
					continue;
				} else if (groupAcc.equals("SAMEG313793")) {
					log.warn("Skipping " + groupAcc + " as it will be removed");
					continue;
				}
*/
				result.add(groupAcc);
			}

			// break;

			// update iterating values
			Element summaryInfo = XMLUtils.getChildByName(root, "SummaryInfo");
			if (summaryInfo == null) log.error("No SummaryInfo found!");
			to = Integer.parseInt(XMLUtils.getChildByName(summaryInfo, "To").getTextTrim());
			total = Integer.parseInt(XMLUtils.getChildByName(summaryInfo, "Total").getTextTrim());
			// move to next page
			page += 1;

		}

		return result;
	}
	
	public void safeHandleGroup(String groupAcc) {
		try {
			handleGroup(groupAcc);
		} catch (Throwable t) {
			log.error("Problem handling "+groupAcc, t);
		}
	}

	public void handleGroup(String groupAcc) {

		log.info("Getting samples for group " + groupAcc + " from BioSamples XML API");
		Set<String> result = new HashSet<>();

		int page = 1;
		Integer to = null;
		Integer total = null;
		while (to == null || to < total) {
			// get an xml for the query
			Document doc = null;
			try {
				doc = XMLUtils.getDocument(
						new URL(urlRoot+"xml/groupsamples/" + groupAcc + "/query=&page=" + page));
			} catch (DocumentException | IOException e) {
				throw new RuntimeException(e);
			}
			Element root = doc.getRootElement();

			// process this pages results
			for (Element sample : XMLUtils.getChildrenByName(root, "BioSample")) {
				String sampleAcc = sample.attributeValue("id");
				log.trace("Found sample " + sampleAcc + " in group " + groupAcc);
				result.add(sampleAcc);

				if (sampleToGroup.containsKey(sampleAcc)) {
					throw new IllegalStateException("Sample " + sampleAcc + " is in multiple groups");
				}
				sampleToGroup.put(sampleAcc, groupAcc);
				handleSample(sampleAcc);
			}

			// update iterating values
			Element summaryInfo = XMLUtils.getChildByName(root, "SummaryInfo");
			to = Integer.parseInt(XMLUtils.getChildByName(summaryInfo, "To").getTextTrim());
			total = Integer.parseInt(XMLUtils.getChildByName(summaryInfo, "Total").getTextTrim());
			// move to next page
			page += 1;
		}
		// check it is sane
		if (result.size() == 0) {
			throw new IllegalStateException("Found zero samples in group " + groupAcc);
		}
		// store in internal maps
		groupToSamples.put(groupAcc, result);

		Document doc = null;
		try {
			doc = XMLUtils.getDocument(new URL(urlRoot+"xml/group/" + groupAcc));
		} catch (DocumentException | IOException e) {
			log.error("Unable to process XML for group " + groupAcc);
			throw new RuntimeException(e);
		}
		Element root = doc.getRootElement();

		// get the name of the sample accession
		String groupName = null;
		for (Element propertyElem : XMLUtils.getChildrenByName(root, "Property")) {
			if ("Group Name".equals(propertyElem.attributeValue("class"))) {
				for (Element qualifiedValueElem : XMLUtils.getChildrenByName(propertyElem, "QualifiedValue")) {
					for (Element valueElem : XMLUtils.getChildrenByName(qualifiedValueElem, "Value")) {
						groupName = valueElem.getTextTrim();
					}
				}
			}
		}
		if (groupName == null) {
			throw new IllegalStateException("Sample " + groupAcc + " has no name");
		}
		accessionToName.put(groupAcc, groupName);

		// get the owner of the sample accession
		Optional<String> owner = accessioner.getUserNameForAccession(groupAcc);
		if (owner.isPresent()) {
			accessionToOwner.put(groupAcc, owner.get());
		}

		// get the submissions of the sample accession
		Set<String> submissions = new HashSet<>();
		submissions.addAll(relationalTemplate.queryForList(
				"SELECT MSI.ACC " + "FROM BIO_SMP_GRP "
						+ "JOIN MSI_SAMPLE_GROUP ON BIO_SMP_GRP.ID = MSI_SAMPLE_GROUP.GROUP_ID "
						+ "JOIN MSI ON MSI_SAMPLE_GROUP.MSI_ID = MSI.ID	" + "WHERE BIO_SMP_GRP.ACC = ?",
				String.class, groupAcc));
		if (submissions.size() > 1) {
			throw new IllegalStateException("Group " + groupAcc + " is in " + submissions.size() + " submissions");
		} else if (submissions.size() == 1) {
			accessionToSubmission.put(groupAcc, submissions.iterator().next());
		}

		accessionToType.put(groupAcc, AccessionType.BATCH);
	}

	public void handleSample(String sampleAcc) {

		Document doc = null;
		try {
			doc = XMLUtils.getDocument(new URL(urlRoot+"xml/sample/" + sampleAcc));
		} catch (DocumentException | IOException e) {
			log.warn("Unable to process XML for sample " + sampleAcc);
			return;
			//throw new RuntimeException("Unable to process XML for sample " + sampleAcc, e);
		}
		Element root = doc.getRootElement();
		for (Element derivedElem : XMLUtils.getChildrenByName(root, "derivedFrom")) {
			String derivedAcc = derivedElem.getTextTrim();

			if (!derivedAcc.matches("SAMEA[0-9]+")) {
				throw new IllegalStateException(
						"derived from is not an accession ( " + sampleAcc + " -> " + derivedAcc + " )");
			}

			if (derivedFrom.containsKey(sampleAcc)) {
				if (derivedFrom.get(sampleAcc).equals(derivedAcc)) {
					// same as an existing dervived from
					// carry on as normal
					continue;
				} else {
					throw new IllegalStateException("Duplicate different derived froms for " + sampleAcc);
				}
			}
			log.trace("Found derived from " + sampleAcc + " <- " + derivedAcc);
			derivedFrom.put(sampleAcc, derivedAcc);
			// store the inverse too
			if (!derivedTo.containsKey(derivedAcc)) {
				derivedTo.put(derivedAcc, new HashSet<>());
			}
			derivedTo.get(derivedAcc).add(sampleAcc);

			// recurse to the new sample, if not already present
			if (!derivedFrom.containsKey(derivedAcc)) {
				handleSample(derivedAcc);
			}
		}

		// get the name of the sample accession
		String sampleName = null;
		for (Element propertyElem : XMLUtils.getChildrenByName(root, "Property")) {
			if ("Sample Name".equals(propertyElem.attributeValue("class"))) {
				for (Element qualifiedValueElem : XMLUtils.getChildrenByName(propertyElem, "QualifiedValue")) {
					for (Element valueElem : XMLUtils.getChildrenByName(qualifiedValueElem, "Value")) {
						sampleName = valueElem.getTextTrim();
					}
				}
			}
		}
		if (sampleName == null) {
			throw new IllegalStateException("Sample " + sampleAcc + " has no name");
		}
		accessionToName.put(sampleAcc, sampleName);

		// get the owner of the sample accession
		Optional<String> owner = accessioner.getUserNameForAccession(sampleAcc);
		if (owner.isPresent()) {
			accessionToOwner.put(sampleAcc, owner.get());
		}

		// get the submissions of the sample accession
		Set<String> submissions = new HashSet<>();
		submissions.addAll(relationalTemplate.queryForList(
				"SELECT MSI.ACC " + "FROM BIO_PRODUCT " + "JOIN MSI_SAMPLE ON BIO_PRODUCT.ID = MSI_SAMPLE.SAMPLE_ID "
						+ "JOIN MSI ON MSI_SAMPLE.MSI_ID = MSI.ID	" + "WHERE BIO_PRODUCT.ACC = ?",
				String.class, sampleAcc));
		if (submissions.size() > 1) {
			throw new IllegalStateException("Sample " + sampleAcc + " is in " + submissions.size() + " submissions");
		} else if (submissions.size() == 1) {
			accessionToSubmission.put(sampleAcc, submissions.iterator().next());
		}

		// try and work out if this is a vial, line, donor, or something else
		// if it has an attribute material:individual then it must be a donor
		for (Element property : XMLUtils.getChildrenByName(root, "Property")) {
			if ("Material".equals(property.attributeValue("class"))) {
				if ("individual".equals(XMLUtils
						.getChildByName(XMLUtils.getChildByName(property, "QualifiedValue"), "Value").getTextTrim())) {
					accessionToType.put(sampleAcc, AccessionType.DONOR);
				}
			}
		}
		// if it has an attribute material:cell line then it must be a cell line
		for (Element property : XMLUtils.getChildrenByName(root, "Property")) {
			if ("Material".equals(property.attributeValue("class"))) {
				if ("cell line".equals(XMLUtils
						.getChildByName(XMLUtils.getChildByName(property, "QualifiedValue"), "Value").getTextTrim())) {
					accessionToType.put(sampleAcc, AccessionType.LINE);
				}
			}
		}
		// if the sample name contains "vial" then it must be a vial
		if (accessionToName.get(sampleAcc).matches(".* vial .*")) {
			accessionToType.put(sampleAcc, AccessionType.VIAL);
		}

		// ensure that it has a type
		if (!accessionToType.containsKey(sampleAcc)) {
			throw new IllegalStateException("Sample " + sampleAcc + " must have a type detected");
		}
	}


	private Set<String> getVials() {
		Set<String> toReturn = new HashSet<>();
		for (String acc : accessionToType.keySet()) {
			if (accessionToType.get(acc).equals(AccessionType.VIAL)) {
				toReturn.add(acc);
			}
		}
		return toReturn;
	}

	// walk up derived from until you find a line object
	public String getLineOfVial(String vialAcc) throws IllegalStateException {
		// TODO validate input
		String acc = vialAcc;
		while (accessionToType.get(acc) == AccessionType.VIAL) {
			if (!derivedFrom.containsKey(acc)) {
				log.error("Accession " + acc + " has derivedFrom");
				return null;
			}
			acc = derivedFrom.get(acc);
			/*
			if (!accessionToType.containsKey(acc)) {
				log.error("Accession " + acc + " has no type");
				return null;
			}
			*/
			// TODO sanity check values
		}
		return acc;
	}

	// walk up derived from until you get to the top
	public String getDonorOfLine(String lineAcc) throws IllegalStateException {
		// TODO validate input
		String acc = lineAcc;
		while (accessionToType.get(acc) != AccessionType.DONOR) {
			if (!derivedFrom.containsKey(acc)) {
				log.error("Accession " + acc + " has no derivedFrom");
				return null;
			}
			acc = derivedFrom.get(acc);
			if (!accessionToType.containsKey(acc)) {
				log.error("Accession " + acc + " has no type");
				return null;
			}
			// TODO sanity check values
		}
		return acc;
	}

	// walk down derived to until you find all vial objects
	public Set<String> getVialsOfLine(String lineAcc) throws IllegalStateException {
		// TODO validate input
		LinkedList<String> accs = new LinkedList<>();
		accs.push(lineAcc);
		Set<String> vialAccs = new HashSet<>();
		while (!accs.isEmpty()) {
			String acc = accs.pop();
			if (!accessionToType.containsKey(acc)) {
				log.error("Accession " + acc + " has no type");
				continue;
			} 
			if (accessionToType.get(acc) == AccessionType.VIAL) {
				vialAccs.add(acc);
			} 
			if (derivedTo.containsKey(acc)){
				accs.addAll(derivedTo.get(acc));
			}			
		}
		return vialAccs;
	}

	private void output() {
		
		// some counting
		Set<String> lineNames = new HashSet<>();

		/// vial vial name vial sub line line sub hESCreg donor donor sub batch
		try (CSVWriter writer = new CSVWriter(
				new FileWriter(csvPath))) {
			writer.writeNext(new String[] { "vial", "vial name now", "vial name wanted", "vial sub", "vial owner",
					"line", "line name", "line sub", "line owner", "donor", "donor sub", "donor owner", "batch",
					"batch name now", "batch name wanted", "batch sub", "batch owner" });
			for (String vialAcc : getVials()) {
				String vialSub = "";
				String vialNameNow = "";
				String vialNameWanted = "";
				String vialOwner = "";

				String lineAcc = "";
				String lineName = "";
				String lineSub = "";
				String lineOwner = "";

				String donorAcc = "";
				String donorSub = "";
				String donorOwner = "";

				String vialBatchAcc = "";
				String vialBatchNameNow = "";
				String vialBatchNameWanted = "";
				String vialBatchSub = "";
				String vialBatchOwner = "";

				vialSub = accessionToSubmission.get(vialAcc);
				vialNameNow = accessionToName.get(vialAcc);
				vialOwner = accessionToOwner.get(vialAcc);

				if (getLineOfVial(vialAcc) != null) {
					lineAcc = getLineOfVial(vialAcc);
					if (accessionToName.containsKey(lineAcc)) {
						lineName = accessionToName.get(lineAcc);
						if (hipsciToEbisc.containsKey(lineName)) {
							lineName = hipsciToEbisc.get(lineName);
						}
						lineSub = accessionToSubmission.get(lineAcc);
						lineOwner = accessionToOwner.get(lineAcc);
	
						if (getDonorOfLine(lineAcc) != null) {
							donorAcc = getDonorOfLine(lineAcc);
							donorSub = accessionToSubmission.get(donorAcc);
							donorOwner = accessionToOwner.get(donorAcc);
						}
					}
				}

				vialBatchAcc = sampleToGroup.get(vialAcc);
				vialBatchNameNow = accessionToName.get(vialBatchAcc);
				vialBatchSub = accessionToSubmission.get(vialBatchAcc);
				vialBatchOwner = accessionToOwner.get(vialBatchAcc);

				// can only construct this after other stuff
				if (accessionToRC.containsKey(vialBatchAcc)) {		
					
					//get the samples in the same batch/group
					List<String> orderedBatchVialAcc = new ArrayList<>();
					//make sure to only get vials
					for (String testAcc : groupToSamples.get(vialBatchAcc)) {
						if (accessionToType.get(testAcc).equals(AccessionType.VIAL)) {
							orderedBatchVialAcc.add(testAcc);
						}
					}
					//renumber vials starting from 1 in each batch
					Collections.sort(orderedBatchVialAcc, new Comparator<String>(){
						@Override
						public int compare(String vialAcc1, String vialAcc2) {
							
							String vialNameNow1 = accessionToName.get(vialAcc1);
							String[] nameSplit1 = vialNameNow1.split(" ");							
							int vialNo1;
							try {
								vialNo1 = Integer.parseInt(nameSplit1[nameSplit1.length-1]);
							} catch (NumberFormatException e) {
								log.error("Unable to get vial number from "+vialNameNow1);
								throw e;
							}
							
							String vialNameNow2 = accessionToName.get(vialAcc2);
							String[] nameSplit2 = vialNameNow2.split(" ");										
							int vialNo2;
							try {
								vialNo2 = Integer.parseInt(nameSplit2[nameSplit2.length-1]);
							} catch (NumberFormatException e) {
								log.error("Unable to get vial number from "+vialNameNow2);
								throw e;
							}
							
							return Integer.compare(vialNo1, vialNo2);
						}});
					
					int vialNoNow = orderedBatchVialAcc.indexOf(vialAcc)+1;
					
					if (lineName.length() > 0) {
						vialNameWanted = lineName + " " + accessionToRC.get(vialBatchAcc) + " vial "+String.format("%04d", vialNoNow);
					}
				}
				if (accessionToRC.containsKey(vialBatchAcc)) {
					vialBatchNameWanted = lineName + " " + accessionToRC.get(vialBatchAcc);
				}

				writer.writeNext(new String[] { vialAcc, vialNameNow, vialNameWanted, vialSub, vialOwner, lineAcc,
						lineName, lineSub, lineOwner, donorAcc, donorSub, donorOwner, vialBatchAcc, vialBatchNameNow,
						vialBatchNameWanted, vialBatchSub, vialBatchOwner });

				// for counting
				if (lineNames != null) {
					lineNames.add(lineName);
				}

			}

		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		log.info("Total number of lines: " + lineNames.size());

	}
}
