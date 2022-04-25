cd ../../src/SG/NS
javac -cp ../Utils.jar:. ./NS.java
rm -f Definitions.class Row_compression.class Col_compression.class
java -cp ../Utils.jar:. NS
