package uk.ac.ebi.fgpt.sampletab;

import org.mged.magetab.error.ErrorItem;
import org.mged.magetab.error.ErrorItemFactory;

import uk.ac.ebi.arrayexpress2.magetab.exception.ValidateException;
import uk.ac.ebi.arrayexpress2.magetab.validator.AbstractValidator;
import uk.ac.ebi.arrayexpress2.sampletab.datamodel.SampleData;

public class BasicValidator<S extends SampleData> extends AbstractValidator<S> {

    private ErrorItemFactory eiFactory = ErrorItemFactory.getErrorItemFactory(getClass().getClassLoader());
    
    public void validate(S st) throws ValidateException {
        
        if (st.scd.getAllNodes().size() == 0){
            fireErrorItemEvent(eiFactory.generateErrorItem("Must include at least one sample or one group", 530, this.getClass()));
        }
        
    }

}
