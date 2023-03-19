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
