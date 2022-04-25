cd ../../src/SG/Bitmap
javac -cp ../Utils.jar ./Bitmap.java
rm -f Definitions.class Row_compression.class Col_compression.class
java -cp ../Utils.jar:. Bitmap
