package uk.ac.ebi.fgpt.sampletab;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class ENAUtils {

	private Logger log = LoggerFactory.getLogger(getClass());

	private static ENAUtils instance = new ENAUtils();

	private static SAXReader reader = new SAXReader();

	private ENAUtils() {
		// private constructor
	}

	public static ENAUtils getInstance() {
		return instance;
	}

	public static Collection<String> getIdentifiers(String input) {
		ArrayList<String> idents = new ArrayList<String>();
		if (input.contains(",")) {
			for (String substr : input.split(",")) {
				idents.add(substr);
			}
		} else {
			idents.add(input);
		}

		ArrayList<String> newidents = new ArrayList<String>();
		for (String ident : idents) {
			if (ident.contains("-")) {
				// its a range
				String[] range = ident.split("-");
				int lower = new Integer(range[0].substring(3));
				int upper = new Integer(range[1].substring(3));
				String prefix = range[0].substring(0, 3);
				for (int i = lower; i <= upper; i++) {
					newidents.add(String.format(prefix + "%06d", i));
				}
			} else {
				newidents.add(ident);
			}
		}

		return newidents;
	}

	public Collection<String> getStudiesForSample(String srsId)
			throws DocumentException {
		String urlstr = "http://www.ebi.ac.uk/ena/data/view/" + srsId
				+ "&display=xml";
		Document doc = reader.read(urlstr);

		Element root = doc.getRootElement();
		Element sample = XMLUtils.getChildByName(root, "SAMPLE");
		Element links = XMLUtils.getChildByName(sample, "SAMPLE_LINKS");
		if (links != null) {
			for (Element link : XMLUtils
					.getChildrenByName(links, "SAMPLE_LINK")) {
				Element xref = XMLUtils.getChildByName(link, "XREF_LINK");
				if (xref != null) {
					Element db = XMLUtils.getChildByName(xref, "DB");
					Element id = XMLUtils.getChildByName(xref, "ID");
					if (db != null && db.getText().equals("ENA-STUDY")
							&& id != null) {
						return getIdentifiers(id.getText());
					}
				}
			}
		}
		// did not match, return null;
		return null;
	}
}
