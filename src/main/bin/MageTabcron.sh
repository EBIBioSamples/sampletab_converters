#!/bin/bash

#Do not modify this file directly
#It is stored at svn://bar.ebi.ac.uk/trunk/fgpt//automation/sampletab-converters/src/main/bin
#and installed by http://coconut.ebi.ac.uk:9081/browse/BSD-CONVS

#ensure files are group writable
umask 002

${0%/*}/sampletab-converters.sh uk.ac.ebi.fgpt.sampletab.arrayexpress.MageTabCron "$@"
