cd ../../src/SG/Collector
javac -cp ../Utils.jar ./Collector.java
rm -f Definitions.class Row_compression.class Col_compression.class
java -cp ../Utils.jar:. Collector
