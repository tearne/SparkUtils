#!/usr/bin/env bash

############################################
# Application specifics
############################################
CLASS=Example
 ARGS="aws-02-compmodelling words.txt"


############################################
# IP address of remote box to run on (if running remotely)
############################################
RUNNER=127.0.0.1


############################################
# Java, Spark and Hadoop dists which will be copied to deploy dir
############################################
  JAVA=/usr/lib/jvm/java-8-openjdk-amd64
 SPARK=$HOME/bin/spark-2.3.0-bin-without-hadoop
HADOOP=$HOME/bin/hadoop-2.8.3


############################################
# Stuff for building
#############################################
DEPLOY_DIR=target/deploy
       LIB=target/universal/stage/lib
 RESOURCES=src/main/resources

ORGANISATION=$(cat build.sbt | grep "organization :=" | cut -f 3 -d " " | tr -d '"')
        NAME=$(cat build.sbt | grep "name :=" | cut -f 3 -d " " | tr -d '"')
     VERSION=$(cat build.sbt | grep "version :=" | cut -f 3 -d " " | tr -d '"')

 APP_JAR=$ORGANISATION.$NAME-$VERSION.jar
