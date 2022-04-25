cd ../../src/SG/Adaptive
javac -cp ../Utils.jar ./Adaptive.java
rm -f Definitions.class Row_compression.class Col_compression.class
java -cp ../Utils.jar:. Adaptive
