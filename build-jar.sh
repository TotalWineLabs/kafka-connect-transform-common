#!/bin/bash

# Simple script to compile Java files and create JAR without Maven
# This is a workaround for Maven compatibility issues

echo "Creating directories..."
mkdir -p target/classes
mkdir -p target/lib

echo "Compiling Java files..."
# You'll need to add the Kafka Connect dependencies to the classpath
# For now, this will just compile what it can
find src/main/java -name "*.java" | xargs javac -d target/classes -cp ".:target/lib/*" 2>/dev/null || echo "Some compilation errors expected due to missing dependencies"

echo "Creating JAR file..."
cd target/classes
jar cf ../kafka-connect-transform-common-0.1.0.62-TWM.jar com/

echo "JAR created: target/kafka-connect-transform-common-0.1.0.62-TWM.jar"
ls -la ../kafka-connect-transform-common-0.1.0.62-TWM.jar