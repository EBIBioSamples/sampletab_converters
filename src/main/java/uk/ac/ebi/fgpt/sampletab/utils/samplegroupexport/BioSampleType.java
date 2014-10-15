//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, vJAXB 2.1.10 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2014.07.22 at 04:51:31 PM BST 
//


package uk.ac.ebi.fgpt.sampletab.utils.samplegroupexport;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for bioSampleType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="bioSampleType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="Annotation" type="{http://www.ebi.ac.uk/biosamples/SampleGroupExport/1.0}annotationType" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="Property" type="{http://www.ebi.ac.uk/biosamples/SampleGroupExport/1.0}propertyType" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="derivedFrom" type="{http://www.ebi.ac.uk/biosamples/SampleGroupExport/1.0}stringValueType" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="Database" type="{http://www.ebi.ac.uk/biosamples/SampleGroupExport/1.0}databaseType" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="GroupIds" type="{http://www.ebi.ac.uk/biosamples/SampleGroupExport/1.0}GroupIdsType" minOccurs="0"/>
 *       &lt;/sequence>
 *       &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "bioSampleType", propOrder = {
    "annotation",
    "property",
    "derivedFrom",
    "database",
    "groupIds"
})
public class BioSampleType {

    @XmlElement(name = "Annotation")
    protected List<AnnotationType> annotation;
    @XmlElement(name = "Property")
    protected List<PropertyType> property;
    protected List<String> derivedFrom;
    @XmlElement(name = "Database")
    protected List<DatabaseType> database;
    @XmlElement(name = "GroupIds")
    protected GroupIdsType groupIds;
    @XmlAttribute(required = true)
    protected String id;

    /**
     * Gets the value of the annotation property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the annotation property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getAnnotation().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link AnnotationType }
     * 
     * 
     */
    public List<AnnotationType> getAnnotation() {
        if (annotation == null) {
            annotation = new ArrayList<AnnotationType>();
        }
        return this.annotation;
    }

    /**
     * Gets the value of the property property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the property property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getProperty().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link PropertyType }
     * 
     * 
     */
    public List<PropertyType> getProperty() {
        if (property == null) {
            property = new ArrayList<PropertyType>();
        }
        return this.property;
    }

    /**
     * Gets the value of the derivedFrom property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the derivedFrom property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getDerivedFrom().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link String }
     * 
     * 
     */
    public List<String> getDerivedFrom() {
        if (derivedFrom == null) {
            derivedFrom = new ArrayList<String>();
        }
        return this.derivedFrom;
    }

    /**
     * Gets the value of the database property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the database property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getDatabase().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link DatabaseType }
     * 
     * 
     */
    public List<DatabaseType> getDatabase() {
        if (database == null) {
            database = new ArrayList<DatabaseType>();
        }
        return this.database;
    }

    /**
     * Gets the value of the groupIds property.
     * 
     * @return
     *     possible object is
     *     {@link GroupIdsType }
     *     
     */
    public GroupIdsType getGroupIds() {
        return groupIds;
    }

    /**
     * Sets the value of the groupIds property.
     * 
     * @param value
     *     allowed object is
     *     {@link GroupIdsType }
     *     
     */
    public void setGroupIds(GroupIdsType value) {
        this.groupIds = value;
    }

    /**
     * Gets the value of the id property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the value of the id property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setId(String value) {
        this.id = value;
    }

}
