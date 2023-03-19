# Remote Object Store

A remote object store that implements a server and client library and communication protocol

Any kind of data be stored as an object, from a string, to pictures, to videos. Within an object store, objects are stored in a flat namespace using a unique identifier or key.

## Architecture and Design

### RUStore Server

The RUStore Serve ris a runnable java program that was implemented in the server class RUStoreServer.java. The main method accepts a single argument which is the port number the server should run on.

### RUStore Client

In order to interact with a remote RUStore Server, the RUStore Client is used.

<img width="816" alt="image" src="https://user-images.githubusercontent.com/107651391/226199047-5e1e2644-90b8-4a41-943f-fc591ab201bf.png">

<img width="714" alt="image" src="https://user-images.githubusercontent.com/107651391/226199332-329e33e5-98ba-4813-9cbe-27f469dc13f5.png">

## Testing

### Test Files
Within the /src/main/java/com/RUStore directory there are three `Test*` files:
- **TestSample.java** - This is a sample test file that will use the RUStore client library and create and try to store and retrieve various objects.
- **TestSandbox.java** - This is an empty file that you can use to implement your own test application that will use the RUStore client library.
- **TestStringCLI.java** - This is a simple interactive program that will allow you to send test and send text to your object server as String objects.

### Test Input Files
There is a folder called `inputfiles` under the root of the project with a file that you can use to test if you can properly store and retrieve files as objects:

**lofi.mp3** - 1 minute of royalty-free lofi music

### Test Output Directory

There is also a folder called `outputfiles`. This is an empty folder where you can direct data objects downloaded from the remote object store. This is simply here to act as a place to keep files separate from the original input files.

## Running
1. Run the server on port 12345
`java -jar ./target/RUStoreServer.jar 12345`

2. Run test programs. Ex.:
`java -jar ./target/TestStringCLI.jar`
