package uk.ac.ebi.fgpt.sampletab.utils;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;

public class TermSourceUtils {

    public static TermSource guessURL(TermSource ts){
        if (ts.getName().equals("EFO")){
            ts = new TermSource(ts.getName(), "http://www.ebi.ac.uk/efo/", null);
        } else if (ts.getName().equals("PAR")){
            ts = new TermSource(ts.getName(), "http://psidev.cvs.sourceforge.net/psidev/psi/mi/controlledVocab/proteomeBinder/psi-par.obo", null);
        } else if (ts.getName().equals("PSI")){
            ts = new TermSource(ts.getName(), "http://psidev.sourceforge.net/ms/xml/mzdata/psi-ms-cv-1.7.2.obo", null);
        } else if (ts.getName().equals("PSI-MS")){
            ts = new TermSource(ts.getName(), "http://psidev.sourceforge.net/ms/xml/mzdata/psi-ms-cv-1.7.2.obo", null);
        } else if (ts.getName().equals("MeSH")){
            ts = new TermSource(ts.getName(), "http://www.nlm.nih.gov/mesh/", null);
        } else if (ts.getName().equals("MP")){
            ts = new TermSource(ts.getName(), "ftp://ftp.informatics.jax.org/pub/reports/MPheno_OBO.ontology", null);
        } else if (ts.getName().equals("PRIDE")){
            ts = new TermSource(ts.getName(), "http://pride-proteome.cvs.sourceforge.net/pride-proteome/PRIDE/schema/pride_cv.obo", null);
        } else if (ts.getName().equals("NEWT")){
            ts = new TermSource(ts.getName(), "http://www.ebi.ac.uk/newt/", null);
        } else if (ts.getName().equals("GPM")){
            ts = new TermSource(ts.getName(), "http://www.thegpm.org/", null);
        } else if (ts.getName().equals("GPMDB")){
            ts = new TermSource(ts.getName(), "http://gpmdb.thegpm.org/", null);
        } else if (ts.getName().equals("MS")){
            ts = new TermSource(ts.getName(), "http://psidev.cvs.sourceforge.net/viewvc/*checkout*/psidev/psi/psi-ms/mzML/controlledVocabulary/psi-ms.obo", null);
        } else if (ts.getName().equals("CCO")){
            ts = new TermSource(ts.getName(), "http://cellcycleonto.cvs.sourceforge.net/cellcycleonto/ONTOLOGIES/OBO/cco.obo", null);
        } else if (ts.getName().equals("MOD")){
            ts = new TermSource(ts.getName(), "http://psidev.cvs.sourceforge.net/psidev/psi/mod/data/PSI-MOD.obo", null);
        } else if (ts.getName().equals("SEP")){
            ts = new TermSource(ts.getName(), "http://code.google.com/p/gelml/source/browse/trunk/CV/sep.obo", null);
        } else if (ts.getName().equals("DOID")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/phenotype/human_disease.obo", null);
        } else if (ts.getName().equals("CL")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/anatomy/cell_type/cell.obo", null);
        } else if (ts.getName().equals("IDO")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/*checkout*/obo/obo/ontology/phenotype/infectious_disease.obo", null);
        } else if (ts.getName().equals("EO")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/phenotype/environment/environment_ontology.obo", null);
        } else if (ts.getName().equals("BTO")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/anatomy/BrendaTissue.obo", null);
        } else if (ts.getName().equals("GO")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/genomic-proteomic/gene_ontology.obo", null);
        } else if (ts.getName().equals("PO")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/developmental/plant_development/plant/po_temporal.obo", null);
        } else if (ts.getName().equals("TAIR")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/developmental/plant_development/Arabidopsis/arabidopsis_development.obo", null);
        } else if (ts.getName().equals("ZEA")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/anatomy/gross_anatomy/plant_gross_anatomy/cereals/zea_mays_anatomy.obo", null);
        } else if (ts.getName().equals("CHEBI")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/chemical/chebi.obo", null);
        } else if (ts.getName().equals("UO")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/phenotype/unit.obo", null);
        } else if (ts.getName().equals("ZFA")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/anatomy/gross_anatomy/animal_gross_anatomy/fish/zebrafish_anatomy.obo", null);
        } else if (ts.getName().equals("TAO")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/anatomy/gross_anatomy/animal_gross_anatomy/fish/teleost_anatomy.obo", null);
        } else if (ts.getName().equals("DDANAT")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/anatomy/gross_anatomy/microbial_gross_anatomy/dictyostelium/dictyostelium_anatomy.obo", null);
        } else if (ts.getName().equals("IMR")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/genomic-proteomic/molecule_role.obo", null);
        } else if (ts.getName().equals("MI")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/genomic-proteomic/protein/psi-mi.obo", null);
        } else if (ts.getName().equals("MIAA")){
            ts = new TermSource(ts.getName(), "http://www.ebi.ac.uk/microarray-as/aer/", null);
        } else if (ts.getName().equals("FBbt")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/anatomy/gross_anatomy/animal_gross_anatomy/fly/fly_anatomy.obo", null);
        } else if (ts.getName().equals("IEV")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/genomic-proteomic/event.obo", null);
        } else if (ts.getName().equals("HP")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/*checkout*/obo/obo/ontology/phenotype/human_phenotype.obo", null);
        } else if (ts.getName().equals("FMA")){
            ts = new TermSource(ts.getName(), "http://obo.svn.sourceforge.net/viewvc/*checkout*/obo/fma-conversion/trunk/fma_obo.obo", null);
        } else if (ts.getName().equals("PATO")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/phenotype/quality.obo", null);
        } else if (ts.getName().equals("TO")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/phenotype/plant_traits/plant_trait.obo", null);
        } else if (ts.getName().equals("FIX")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/physicochemical/fix.obo", null);
        } else if (ts.getName().equals("FBsp")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/taxonomy/fly_taxonomy.obo", null);
        } else if (ts.getName().equals("GRO")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/developmental/plant_development/cereals/cereals_development.obo", null);
        } else if (ts.getName().equals("EHDA")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/anatomy/gross_anatomy/animal_gross_anatomy/human/human-dev-anat-staged.obo", null);
        } else if (ts.getName().equals("PRO")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/genomic-proteomic/pro.obo", null);
        } else if (ts.getName().equals("MA")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/anatomy/gross_anatomy/animal_gross_anatomy/mouse/adult_mouse_anatomy.obo", null);
        } else if (ts.getName().equals("MPATH")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/phenotype/mouse_pathology/mouse_pathology.obo", null);
        } else if (ts.getName().equals("SO")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/genomic-proteomic/so.obo", null);
        } else if (ts.getName().equals("TTO")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/*checkout*/obo/obo/ontology/taxonomy/teleost_taxonomy.obo", null);
        } else if (ts.getName().equals("ZDB")){
            ts = new TermSource(ts.getName(), "http://obo.cvs.sourceforge.net/obo/obo/ontology/anatomy/gross_anatomy/animal_gross_anatomy/fish/zebrafish_anatomy.obo", null);
        } else if (ts.getName().equals("")){
            ts = new TermSource(ts.getName(), "", null);
        } else if (ts.getName().equals("")){
            ts = new TermSource(ts.getName(), "", null);
        } else if (ts.getName().equals("")){
            ts = new TermSource(ts.getName(), "", null);
        } else if (ts.getName().equals("")){
            ts = new TermSource(ts.getName(), "", null);
        } else if (ts.getName().equals("")){
            ts = new TermSource(ts.getName(), "", null);
        } else if (ts.getName().equals("")){
            ts = new TermSource(ts.getName(), "", null);
        } else if (ts.getName().equals("")){
            ts = new TermSource(ts.getName(), "", null);
        } else if (ts.getName().equals("")){
            ts = new TermSource(ts.getName(), "", null);
        } else if (ts.getName().equals("")){
            ts = new TermSource(ts.getName(), "", null);
        } else {
            return null;
        }
        return ts;
    }
}
