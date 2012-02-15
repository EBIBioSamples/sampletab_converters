package uk.ac.ebi.fgpt.sampletab;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;

public class Corrector {

    public static String getInitialCapitals(String in){
        StringBuilder sb = new StringBuilder();
        boolean space = true;
        for (Character currentChar : in.toCharArray()) {
            switch (currentChar) {
                case ' ':
                    space = true;
                    sb.append(currentChar);
                    break;
                default:
                    if (space){
                        sb.append(Character.toUpperCase(currentChar));
                        space = false;
                    } else {
                        sb.append(currentChar);
                    }
                    break;
            }
        }
        return sb.toString();
    }
    
    public static void correct(SampleData st) {
        for (SampleNode s : st.scd.getNodes(SampleNode.class)) {
            for (SCDNodeAttribute a : s.getAttributes()) {
                // tidy all characteristics
                if (CharacteristicAttribute.class.isInstance(a)) {
                    CharacteristicAttribute cha = (CharacteristicAttribute) a;
                    // remove not applicables
                    if (cha.getAttributeValue().toLowerCase().equals("n/a")
                            || cha.getAttributeValue().toLowerCase().equals("na")
                            || cha.getAttributeValue().toLowerCase().equals("none")
                            || cha.getAttributeValue().toLowerCase().equals("unknown")
                            || cha.getAttributeValue().toLowerCase().equals("unknown_sex")
                            || cha.getAttributeValue().toLowerCase().equals("--")) {
                        s.removeAttribute(cha);
                        continue;
                    }
                    // make organism a separate attribute
                    if (cha.type.toLowerCase().equals("organism") 
                            || cha.type.toLowerCase().equals("organi")) {
                        s.removeAttribute(cha);
                        OrganismAttribute orga = new OrganismAttribute(cha.getAttributeValue());
                        orga.setTermSourceID(cha.termSourceID);
                        orga.setTermSourceREF(cha.termSourceREF);
                        // TODO check use of NCBI Taxonomy
                        // TODO validate against taxonomy
                        s.addAttribute(orga);
                    }
                    // fix typos
                    if (cha.type.toLowerCase().equals("gender") 
                            || cha.type.toLowerCase().equals("sex")) {
                        cha.type = "Sex";
                        if (cha.getAttributeValue().toLowerCase().equals("male")
                                || cha.getAttributeValue().toLowerCase().equals("female")
                                || cha.getAttributeValue().toLowerCase().equals("m")
                                || cha.getAttributeValue().toLowerCase().equals("f")) {
                            cha.setAttributeValue(cha.getAttributeValue().toLowerCase());
                        }
                    } else if (cha.type.toLowerCase().equals("age")) {
                        cha.type = getInitialCapitals(cha.type);
                    } else if (cha.type.toLowerCase().equals("cell line") 
                            || cha.type.toLowerCase().equals("cellline")) {
                        cha.type = "Cell Line";
                    } else if (cha.type.toLowerCase().equals("developmental stage")
                            || cha.type.toLowerCase().equals("developmentalstage")) {
                        cha.type = "Developmental Stage";
                    } else if (cha.type.toLowerCase().equals("disease state")
                            || cha.type.toLowerCase().equals("diseasestate")) {
                        cha.type = "Disease State";
                    } else if (cha.type.toLowerCase().equals("ecotype")) {
                        cha.type = getInitialCapitals(cha.type);
                    } else if (cha.type.toLowerCase().equals("ethnicity")) {
                        cha.type = getInitialCapitals(cha.type);
                        //TODO combine race with ethnicity in humans
                    } else if (cha.type.toLowerCase().equals("genotype")) {
                        cha.type = getInitialCapitals(cha.type);
                    } else if (cha.type.toLowerCase().equals("histology")) {
                        cha.type = getInitialCapitals(cha.type);
                    } else if (cha.type.toLowerCase().equals("individual")) {
                        //TODO investigate
                        cha.type = getInitialCapitals(cha.type);
                    } else if (cha.type.toLowerCase().equals("organism part") 
                            ||cha.type.toLowerCase().equals("organismpart")) {
                        cha.type = "Organism Part";
                    } else if (cha.type.toLowerCase().equals("phenotype")) {
                        cha.type = getInitialCapitals(cha.type);
                    } else if (cha.type.toLowerCase().equals("stage")) {
                        cha.type = getInitialCapitals(cha.type);
                    } else if (cha.type.toLowerCase().equals("strain")
                            ||cha.type.toLowerCase().equals("strainorline")) {
                        cha.type = "StrainOrLine";
                    } else if (cha.type.toLowerCase().equals("time")
                            || cha.type.toLowerCase().equals("time point")) {
                        cha.type = "Time Point";
                        //TODO fix "Time Unit" being a separate characteristic
                        //TODO fix embedding of units in the string (e.g. 24h) 
                    } else if (cha.type.toLowerCase().equals("tissue")) {
                        cha.type = getInitialCapitals(cha.type);
                        if (cha.getAttributeValue().toLowerCase().equals("liver")
                                || cha.getAttributeValue().toLowerCase().equals("blood")) {
                            cha.setAttributeValue(cha.getAttributeValue().toLowerCase());
                        }
                    } 
                }
            }
        }
    }
}
