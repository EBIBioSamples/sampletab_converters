#!/bin/bash

#Do not modify this file directly
#It is stored at svn://bar.ebi.ac.uk/trunk/fgpt//automation/sampletab-converters/src/main/bin
#and installed by http://coconut.ebi.ac.uk:9081/browse/BSD-CONVS

#Trim out the assay part of the PRIDE XML files
#If this is not done, they are too large to be used effectively.

gunzip -cd "$1" | sed '/<GelFreeIdentification>/,/<\/GelFreeIdentification>/d' | sed '/<TwoDimensionalIdentification>/,/<\/TwoDimensionalIdentification>/d' | sed '/<spectrumList count=\"[0-9]+\">/,/<\\/spectrumList>/d' > "$2"
