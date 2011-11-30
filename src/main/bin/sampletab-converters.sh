#!/bin/sh

base=${0%/*}/..;
current=`pwd`;

#if a java environment variable is not provided, then use the default
if [ -z $java ]
then
  java=java
fi

#args environment variable can be used to provide java arguments

for file in `ls $base/lib`
do
  jars=$jars:$base/lib/$file;
done

classpath="$jars:$base/config";

$java $args -classpath $classpath $@
