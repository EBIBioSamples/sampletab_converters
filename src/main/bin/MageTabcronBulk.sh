#!/bin/bash

#Do not modify this file directly
#It is stored at svn://bar.ebi.ac.uk/trunk/fgpt//automation/sampletab-converters/src/main/bin
#and installed by http://coconut.ebi.ac.uk:9081/browse/BSD-CONVS

echo "This script is deprecated. Use MageTabBulk.sh instead."

${0%/*}/sampletab-converters.sh uk.ac.ebi.fgpt.sampletab.arrayexpress.MageTabBulk "$@"
