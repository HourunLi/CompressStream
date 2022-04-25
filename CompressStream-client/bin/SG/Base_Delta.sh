cd ../../src/SG/Base_Delta
javac -cp ../Utils.jar:. ./Base_Delta.java
rm -f Definitions.class Row_compression.class Col_compression.class
java -cp ../Utils.jar:. Base_Delta
