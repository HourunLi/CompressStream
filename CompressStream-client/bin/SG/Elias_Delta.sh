cd ../../src/SG/Elias_Delta
javac -cp ../Utils.jar:. ./Elias_Delta.java
rm -f Definitions.class Row_compression.class Col_compression.class
java -cp ../Utils.jar:. Elias_Delta
