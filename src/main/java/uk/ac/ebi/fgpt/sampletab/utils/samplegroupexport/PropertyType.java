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
 * <p>Java class for propertyType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="propertyType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="QualifiedValue" type="{http://www.ebi.ac.uk/biosamples/SampleGroupExport/1.0}qualifiedValueType" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *       &lt;attribute name="class" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="characteristic" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="comment" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="type" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "propertyType", propOrder = {
    "qualifiedValue"
})
public class PropertyType {

    @XmlElement(name = "QualifiedValue")
    protected List<QualifiedValueType> qualifiedValue;
    @XmlAttribute(name = "class", required = true)
    protected String clazz;
    @XmlAttribute
    protected Boolean characteristic;
    @XmlAttribute
    protected Boolean comment;
    @XmlAttribute
    protected String type;

    /**
     * Gets the value of the qualifiedValue property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the qualifiedValue property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getQualifiedValue().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link QualifiedValueType }
     * 
     * 
     */
    public List<QualifiedValueType> getQualifiedValue() {
        if (qualifiedValue == null) {
            qualifiedValue = new ArrayList<QualifiedValueType>();
        }
        return this.qualifiedValue;
    }

    /**
     * Gets the value of the clazz property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getClazz() {
        return clazz;
    }

    /**
     * Sets the value of the clazz property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setClazz(String value) {
        this.clazz = value;
    }

    /**
     * Gets the value of the characteristic property.
     * 
     * @return
     *     possible object is
     *     {@link Boolean }
     *     
     */
    public Boolean isCharacteristic() {
        return characteristic;
    }

    /**
     * Sets the value of the characteristic property.
     * 
     * @param value
     *     allowed object is
     *     {@link Boolean }
     *     
     */
    public void setCharacteristic(Boolean value) {
        this.characteristic = value;
    }

    /**
     * Gets the value of the comment property.
     * 
     * @return
     *     possible object is
     *     {@link Boolean }
     *     
     */
    public Boolean isComment() {
        return comment;
    }

    /**
     * Sets the value of the comment property.
     * 
     * @param value
     *     allowed object is
     *     {@link Boolean }
     *     
     */
    public void setComment(Boolean value) {
        this.comment = value;
    }

    /**
     * Gets the value of the type property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the value of the type property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setType(String value) {
        this.type = value;
    }

}
