cd ../../src/SG/DICT
javac -cp ../Utils.jar:. ./DICT.java
rm -f Definitions.class Row_compression.class Col_compression.class
java -cp ../Utils.jar:. DICT
