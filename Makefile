LIB_PATH=lib/protobuf-java-3.4.1.jar
all: clean
	mkdir bin
	javac -classpath $(LIB_PATH) -d bin src/Bank.java src/BranchServer.java src/Controller.java

clean:
	rm -rf bin
