cd ../../src/SG/RLE
javac -cp ../Utils.jar:. ./RLE.java
rm -f Definitions.class Row_compression.class Col_compression.class
java -cp ../Utils.jar:. RLE
