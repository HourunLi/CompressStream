cd ../../src/SG/Baseline
javac -cp ../Utils.jar ./Baseline.java
rm -f Definitions.class Row_compression.class Col_compression.class
java -cp ../Utils.jar:. Baseline
