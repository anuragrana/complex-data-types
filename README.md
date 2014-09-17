#Complex Data Types

Aerospike supports complex Bin types of List and Map. A list can contain other Lists and Maps and a Map can contain other Lists and Maps.

## Lists
List is a collections of values ordered in insert order. A list may contain values of any of the supported data-types, including other Lists and Maps. The size is limited only by the maximum record size. (default of 128k)

A list is ideally suited to storing a JSON Array and is store internally in a language neutral way. So a list can be written in C# and read in Python with no issues. 

By default, you store and retrieve the whole list from the client API. So do consider the network cost when using sizable lists. 

Lists are rendered into msgpack for local storage. Lists are serialized on the client, and sent to the server using the wire protocol. 

##Map
Map is a collection of key-value pairs, such that each key may only appear once in the collection and is associated with a value. The key and value of a map may be of any of the supported data-types, including other Lists and Maps. The size is limited only by the maximum record size.

A Map is ideally suited to storing a JSON Object and is store internally in a language neutral way. So a Map can be written in Java and read in Go with no issues. 

Like a list, you store and retrieve the whole map from the client API. So do consider the network cost when using sizable Maps. 

Maps are rendered into msgpack for local storage. Maps are serialized on the client, and sent the server using the wire protocol. 

##JSON
A combination of Lists and Maps allows you to store JSON documents. It is better to use a List/Map combination rather than a JSON document as a string.

# Document UDF
Reading a sizable document into the Client, modifying it, and storing it again, works very well, but has the cost of the network latencies.

Rather than reading the "elephant" from the database, painting it's toe nail pink  in you application, and writing it back into the database, why not a way to just paint it's toe nail in the database.

The `document.lua` file contains a UDF module to get and set individual elements in a document stored in a Bin. The elements can be of any supported database type:
* String
* Integer (unsigned 64 bit)
* ByteArray (BLOB)
* List
* Map

## Installation
Download the `document.lua` and register it with aql:
```sql
register module '<your directory./document.lua'
```
aql will register the module with all of the nodes in your cluster.

## Functions
### get()
The get() function gets a value from a document. The document is an Aerospike List or Map, containing other Lists and/or Maps, stored in a Bin.
####
Parameters:
* record - The record containing the document Bin
* docBin - The name of the document Bin
* path - A list representing a path to the element to be modified. This is an Aerospike List.

### set()
The set() function sets a value in a document. The document is an Aerospike List or Map, containing other Lists and/or Maps, stored in a Bin. 
Parameters: 
* record - The record containing the document Bin
* docBin - The name of the document Bin
* path - A list representing a path to the element to be modified. This is an Aerospike List.
* value - The value to be 'set' at the location specified by path

## Example Java code
The example Java code demonstrates how to call the UDFs in the `document.lua` module. There are several examples of complex types nested inside one another including a JSON array and a JSON object.

To build the Java example run this command:
```bash
mvn clean package
```
It will produce a runnable JAR in the `target` subdirectory:

`target/complex-data-types-1.0.0-jar-with-dependencies.jar`

You can run this JAR with:
```bash
java -jar complex-data-types-1.0.0-jar-with-dependencies.jar -h <cluster ip address>
```
This example code will register the `document.lua` UDF module each time it is run. It will look for the UDF module in a subdirectory named `udf`.  


