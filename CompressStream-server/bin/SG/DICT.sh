cd ../../example/SG/Baseline
make
cd ../../../src/SG/DICT
javac -cp ./:../../../bin/disruptor-3.2.1.jar:../Utils.jar ./Main.java
cd ../../../bin
java -Xms12g -Xmx12g -cp ./disruptor-3.2.1.jar:../src/SG/DICT/:../src/SG/Utils.jar Main

# jar src -> bin
# example main.cpp -> query1.cpp
# header
