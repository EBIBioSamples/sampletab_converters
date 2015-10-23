#!/bin/bash

#Do not modify this file directly
#It is stored at svn://bar.ebi.ac.uk/trunk/fgpt//automation/sampletab-converters/src/main/bin
#and installed by http://coconut.ebi.ac.uk:9081/browse/BSD-CONVS

base=${0%/*}/..;
current=`pwd`;

#ensure files are group writable
umask 002

#If a java environment variable is not provided, then use a default
if [ -z $java ]
then
  java="/ebi/research/software/Linux_x86_64/opt/java/jdk1.8/bin/java"
fi

#args environment variable can be used to provide java arguments

#add some memory management
if [ -z "$args" ]
then
  args="-Xmx16g -XX:+UseConcMarkSweepGC"
fi
	
#add proxy args
args="$args -Dhttp.proxyHost=wwwcache.ebi.ac.uk -Dhttp.proxyPort=3128 -Dhttp.nonProxyHosts=*.ebi.ac.uk -DproxyHost=wwwcache.ebi.ac.uk -DproxyPort=3128 -DproxySet=true -Djava.net.preferIPv4Stack=true"


#Combine jar files used into one variable
for file in `ls $base/lib`
do
  jars=$jars:$base/lib/$file;
done
#trim leading colon
jars=${jars:1}

#Make sure the classpath contains jar to run
#and other dependent jars
classpath="$jars";

#Make sure files are group-writeable
#mostly used when files are automatically generated, e.g. from cron
umask 002

$java $args -classpath $classpath "$@"

exit $?
