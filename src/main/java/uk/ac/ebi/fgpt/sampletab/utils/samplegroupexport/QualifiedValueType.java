//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, vJAXB 2.1.10
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a>
// Any modifications to this file will be lost upon recompilation of the source schema.
// Generated on: 2014.11.04 at 05:25:28 PM GMT
//

package uk.ac.ebi.fgpt.sampletab.utils.samplegroupexport;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 * <p>
 * Java class for qualifiedValueType complex type.
 * 
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="qualifiedValueType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="Value" type="{http://www.ebi.ac.uk/biosamples/SampleGroupExport/1.0}stringValueType"/>
 *         &lt;element name="TermSourceREF" type="{http://www.ebi.ac.uk/biosamples/SampleGroupExport/1.0}termSourceREFType" minOccurs="0"/>
 *         &lt;element name="Unit" type="{http://www.ebi.ac.uk/biosamples/SampleGroupExport/1.0}stringValueType" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "qualifiedValueType", propOrder = { "value", "termSourceREF", "unit" })
public class QualifiedValueType {

    @XmlElement(name = "Value", required = true)
    protected String value;
    @XmlElement(name = "TermSourceREF")
    protected TermSourceREFType termSourceREF;
    @XmlElement(name = "Unit")
    protected String unit;

    /**
     * Gets the value of the value property.
     * 
     * @return
     *         possible object is {@link String }
     * 
     */
    public String getValue() {
        return value;
    }

    /**
     * Sets the value of the value property.
     * 
     * @param value
     *            allowed object is {@link String }
     * 
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Gets the value of the termSourceREF property.
     * 
     * @return
     *         possible object is {@link TermSourceREFType }
     * 
     */
    public TermSourceREFType getTermSourceREF() {
        return termSourceREF;
    }

    /**
     * Sets the value of the termSourceREF property.
     * 
     * @param value
     *            allowed object is {@link TermSourceREFType }
     * 
     */
    public void setTermSourceREF(TermSourceREFType value) {
        this.termSourceREF = value;
    }

    /**
     * Gets the value of the unit property.
     * 
     * @return
     *         possible object is {@link String }
     * 
     */
    public String getUnit() {
        return unit;
    }

    /**
     * Sets the value of the unit property.
     * 
     * @param value
     *            allowed object is {@link String }
     * 
     */
    public void setUnit(String value) {
        this.unit = value;
    }

}
