libtest.so: Base_Delta.cpp ../../../src/util.cpp
	g++ -O3  Base_Delta.cpp ../../../src/util.cpp -fPIC -shared -o libtest.so -I/usr/lib/jvm/java-8-openjdk-amd64/include/ -I/usr/lib/jvm/java-8-openjdk-amd64/include/linux/ -I/opt/AMDAPP/include -L/opt/AMDAPP/lib/x86_64 -lOpenCL 
# 	g++ -O3  Base_Delta.cpp ../../src/util.cpp -fPIC -shared -o libtest.so -I/Library/Java/JavaVirtualMachines/jdk1.8.0_261.jdk/Contents/Home/include/ -I/Library/Java/JavaVirtualMachines/jdk1.8.0_261.jdk/Contents/Home/include/darwin/ -I/opt/AMDAPP/include -L/Library/Developer/CommandLineTools/SDKs/MacOSX10.15.sdk/System/Library/Frameworks/OpenCL.framework/Versions/A -framework OpenCL
clean:
	rm -f libtest.so
