cd ../../example/SG/Base_Delta
make
cd ../../../src/SG/Base_Delta
javac -cp ./:../../../bin/disruptor-3.2.1.jar:../Utils.jar ./Main.java
cd ../../../bin
java -Xms12g -Xmx12g -cp ./disruptor-3.2.1.jar:../src/SG/Base_Delta/:../src/SG/Utils.jar Main

# jar src -> bin
# example main.cpp -> query1.cpp
# header
