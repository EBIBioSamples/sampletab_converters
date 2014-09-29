package uk.ac.ebi.fgpt.sampletab;

import java.util.ArrayList;
import java.util.List;

import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Database;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Organization;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Person;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.Publication;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.msi.TermSource;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.SampleNode;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.CommentAttribute;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.scd.node.attribute.SCDNodeAttribute;

public class Normalizer {

    public void normalize(SampleData sd) {

        List<Organization> organizations = new ArrayList<Organization>();
        for (Organization organization : sd.msi.organizations) {
            if (organization != null && !organizations.contains(organization)) {
                organizations.add(organization);
            }
        }
        sd.msi.organizations = organizations;

        List<Person> persons = new ArrayList<Person>();
        for (Person person : sd.msi.persons) {
            if (person != null && !persons.contains(person)) {
                persons.add(person);
            }
        }
        sd.msi.persons = persons;

        List<TermSource> termSources = new ArrayList<TermSource>();
        for (TermSource termSource : sd.msi.termSources) {
            if (termSource != null && !termSources.contains(termSource)) {
                termSources.add(termSource);
            }
        }
        sd.msi.termSources = termSources;

        List<Publication> publications = new ArrayList<Publication>();
        for (Publication publication : sd.msi.publications) {
            if (publication != null && !publications.contains(publication)) {
                publications.add(publication);
            }
        }
        sd.msi.publications = publications;

        List<Database> dbs = new ArrayList<Database>();
        for (Database db : sd.msi.databases) {
            if (db != null && !dbs.contains(db)) {
                dbs.add(db);
            }
        }
        sd.msi.databases = dbs;
        
        
        for (SampleNode s : sd.scd.getNodes(SampleNode.class)) {
            for (SCDNodeAttribute a : new ArrayList<SCDNodeAttribute>(s.getAttributes())) {
                boolean isCommentAttribute = false;
                synchronized(CommentAttribute.class){
                    isCommentAttribute = CommentAttribute.class.isInstance(a);
                }
                if (isCommentAttribute) {
                    CommentAttribute ca = (CommentAttribute) a;
                    //remove synonym comments that duplicate sample name
                    if (ca.type.toLowerCase().equals("synonym") && ca.getAttributeValue().equals(s.getNodeName())) {
                        s.removeAttribute(ca);
                    }
                }
            }
        }
    }
}
