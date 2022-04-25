cd ../../src/SG/NSV
javac -cp ../Utils.jar:. ./NSV.java
rm -f Definitions.class Row_compression.class Col_compression.class
java -cp ../Utils.jar:. NSV
