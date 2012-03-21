package uk.ac.ebi.fgpt.sampletab;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.AbstractNodeAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CharacteristicAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.OrganismAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SexAttribute;

public class Corrector {
    // logging
    private static Logger log = LoggerFactory.getLogger("uk.ac.ebi.fgpt.sampletab.Corrector");

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
            //convert to array so we can delete and add attributes if needed
            for (Object a : s.getAttributes().toArray()) {
                boolean isAbstractSCDAttribute = false;
                synchronized(AbstractNodeAttribute.class){
                    isAbstractSCDAttribute = AbstractNodeAttribute.class.isInstance(a);
                }
                boolean isCharacteristic = false;
                synchronized(CharacteristicAttribute.class){
                    isCharacteristic = CharacteristicAttribute.class.isInstance(a);
                }
                boolean isSex = false;
                synchronized(SexAttribute.class){
                    isSex = SexAttribute.class.isInstance(a);
                }

                // tidy things that apply to all attributes
                if (isAbstractSCDAttribute) {
                    AbstractNodeAttribute cha = (AbstractNodeAttribute) a;
                    // remove not applicables
                    if (cha.getAttributeValue().toLowerCase().equals("n/a")
                            || cha.getAttributeValue().toLowerCase().equals("na")
                            || cha.getAttributeValue().toLowerCase().equals("none")
                            || cha.getAttributeValue().toLowerCase().equals("unknown")
                            || cha.getAttributeValue().toLowerCase().equals("--")) {
                        //leave unknown-sex as is. implies it has been looked at and is non-determinate
                        s.removeAttribute(cha);
                        continue;
                    }
                }
                
                // tidy all characteristics
                if (isCharacteristic) {
                    CharacteristicAttribute cha = (CharacteristicAttribute) a;
                    
                    //bulk replace underscore with space in types
                    cha.type = cha.type.replace("_", " ");

                    //remove technical attributes
                    if (cha.type.toLowerCase().equals("channel")){
                        s.removeAttribute(cha);
                        continue;
                    }
                                        
                    // make organism a separate attribute
                    if (cha.type.toLowerCase().equals("organism") 
                            || cha.type.toLowerCase().equals("organi") //from ArrayExpress
                            || cha.type.toLowerCase().equals("arrayexpress-species") //from ENA SRA
                            || cha.type.toLowerCase().equals("cell organism") //from ENA SRA
                            ) {
                        s.removeAttribute(cha);
                        OrganismAttribute orga = new OrganismAttribute(cha.getAttributeValue());
                        orga.setTermSourceID(cha.getTermSourceID());
                        orga.setTermSourceREF(cha.getTermSourceREF());
                        // TODO check use of NCBI Taxonomy
                        // TODO validate against taxonomy
                        s.addAttribute(orga);
                        continue;
                    }
                    // make sex a separate attribute
                    if (cha.type.toLowerCase().equals("sex") 
                            || cha.type.toLowerCase().equals("gender")
                            || cha.type.toLowerCase().equals("arrayexpress-sex") //from ENA SRA
                            || cha.type.toLowerCase().equals("cell sex") //from ENA SRA
                            ) {
                        s.removeAttribute(cha);
                        SexAttribute sexa = new SexAttribute();
                        //NB these are also done below in the section dealing directly with SexAttribute
                        if (cha.getAttributeValue().toLowerCase().equals("male")
                                || cha.getAttributeValue().toLowerCase().equals("m")) {
                            sexa.setAttributeValue("male");
                        } else if (cha.getAttributeValue().toLowerCase().equals("female")
                                || cha.getAttributeValue().toLowerCase().equals("f")) {
                            sexa.setAttributeValue("female");
                        } else {
                            sexa.setAttributeValue(cha.getAttributeValue());
                        }
                        sexa.setTermSourceID(cha.getTermSourceID());
                        sexa.setTermSourceREF(cha.getTermSourceREF());
                        // TODO check use of EFO
                        // TODO validate against EFO
                        s.addAttribute(sexa);
                        continue;
                    }
                    
                    
                    //TODO make material a separate attribute
                    
                    // fix typos
                    if (cha.type.toLowerCase().equals("age")) {
                        cha.type = getInitialCapitals(cha.type);
                        //TODO some simple regex expansions, e.g. 5W to 5 weeks
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
                        if (cha.getAttributeValue().toLowerCase().equals("col-0")
                                || cha.getAttributeValue().toLowerCase().equals("columbia-0")
                                || cha.getAttributeValue().toLowerCase().equals("columbia (col0) ")){
                                cha.setAttributeValue("Columbia-0");
                        } else if (cha.getAttributeValue().toLowerCase().equals("columbia")
                                || cha.getAttributeValue().toLowerCase().equals("col")) {
                            cha.setAttributeValue("Columbia");
                        }
                    } else if (cha.type.toLowerCase().equals("ethnicity")) {
                        cha.type = getInitialCapitals(cha.type);
                        //ethnicity, population, race are a mess, leave alone
                    } else if (cha.type.toLowerCase().equals("genotype")
                            ||cha.type.toLowerCase().equals("individualgeneticcharacteristics")
                            ||cha.type.toLowerCase().equals("genotype/variation") ) {
                        cha.type = "Genotype";
                        if (cha.getAttributeValue().toLowerCase().equals("wildtype")
                                || cha.getAttributeValue().toLowerCase().equals("wild type")
                                || cha.getAttributeValue().toLowerCase().equals("wild-type")
                                || cha.getAttributeValue().toLowerCase().equals("wild_type")
                                || cha.getAttributeValue().toLowerCase().equals("wt")) {
                            cha.setAttributeValue("wild type");
                        }
                    } else if (cha.type.toLowerCase().equals("histology")) {
                        cha.type = getInitialCapitals(cha.type);
                    } else if (cha.type.toLowerCase().equals("individual")) {
                        //TODO investigate
                        cha.type = getInitialCapitals(cha.type);
                    } else if (cha.type.toLowerCase().equals("organism part") 
                            ||cha.type.toLowerCase().equals("organismpart")) {
                        cha.type = "Organism Part";
                        if (cha.getAttributeValue().toLowerCase().equals("blood")){
                            cha.setAttributeValue("blood");
                        } else if (cha.getAttributeValue().toLowerCase().equals("skin")){
                            cha.setAttributeValue("skin");
                        } else if (cha.getAttributeValue().toLowerCase().equals("bone marrow")){
                            cha.setAttributeValue("bone marrow");
                        }
                    } else if (cha.type.toLowerCase().equals("phenotype")) {
                        cha.type = getInitialCapitals(cha.type);
                    } else if (cha.type.toLowerCase().equals("stage")) {
                        cha.type = getInitialCapitals(cha.type);
                    } else if (cha.type.toLowerCase().equals("strain")
                            || cha.type.toLowerCase().equals("strainorline")
                            || cha.type.toLowerCase().equals("cell line")
                            || cha.type.toLowerCase().equals("arrayexpress-strainorline")) {
                        //Leave alone cultivate and ecotype
                        cha.type = "StrainOrLine";
                    } else if (cha.type.toLowerCase().equals("time")
                            || cha.type.toLowerCase().equals("time point")) {
                        cha.type = "Time Point";
                        //TODO fix "Time Unit" being a separate characteristic
                        //TODO fix embedding of units in the string (e.g. 24h) 
                    } else if (cha.type.toLowerCase().equals("tissue")) {
                        cha.type = getInitialCapitals(cha.type);
                        if (cha.getAttributeValue().toLowerCase().equals("liver")
                                || cha.getAttributeValue().toLowerCase().equals("blood")
                                || cha.getAttributeValue().toLowerCase().equals("breast")) {
                            cha.setAttributeValue(cha.getAttributeValue().toLowerCase());
                        }
                    } else if (cha.type.toLowerCase().equals("cell type")
                            || cha.type.toLowerCase().equals("celltype")) {
                        cha.type = "Cell Type";
                        if (cha.getAttributeValue().toLowerCase().equals("liver")
                                || cha.getAttributeValue().toLowerCase().equals("blood")
                                || cha.getAttributeValue().toLowerCase().equals("breast")) {
                            cha.setAttributeValue(cha.getAttributeValue().toLowerCase());
                        }
                    }
                    //TODO HTML URL encoding e.g. %3E %apos; %quot;
                    
                    //TODO demote some characteristics to comments
                } else if (isSex) {
                    SexAttribute sexa = (SexAttribute) a;
                    if (sexa.getAttributeValue().toLowerCase().equals("male")
                            || sexa.getAttributeValue().toLowerCase().equals("m")) {
                        sexa.setAttributeValue("male");
                    } else if (sexa.getAttributeValue().toLowerCase().equals("female")
                            || sexa.getAttributeValue().toLowerCase().equals("f")) {
                        sexa.setAttributeValue("female");
                    }
                }
                //TODO comments
                //TODO promote some comments to characteristics
            }
        }
    }
}
