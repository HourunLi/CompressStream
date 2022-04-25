cd ../../src/SG/Elias_Gamma
javac -cp ../Utils.jar:. ./Elias_Gamma.java
rm -f Definitions.class Row_compression.class Col_compression.class
java -cp ../Utils.jar:. Elias_Gamma
