# DynamoDB Subset


For furthur information about Amazon's DynamoDB, read and understand this paper: [DynamoDB paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)

## Data Types
`Dynamo_Types.go`:
```golang
//Placeholder type for RPC functions that don't need an argument list or a return value
type Empty struct {}

//Context associated with some value
type Context struct {
    Clock VectorClock
}

//Information needed to connect to a DynamoNOde
type DynamoNode struct {
    Address string
    Port    string
}

//A single value, as well as the Context associated with it
type ObjectEntry struct {
    Context Context
    Value  []byte
}

//Result of a Get operation, a list of ObjectEntry structs
type DynamoResult struct {
    EntryList []ObjectEntry
}

//Arguments required for a Put operation: the key, the context, and the value
type PutArgs struct {
    Key     string
    Context Context
    Value  []byte
}
```
These types are intended to be used for the RPC interfaces, so please *do not modify them*. However, feel free to add other types to the `Dynamo_Types.go` file if you feel the need.

## Dynamo Nodes
This file defines an RPC interface for a Dynamo node. As a result, please *do not modify the signatures of the given functions and methods*. 
Additionally, feel free to add members to the DynamoServer struct as you see fit, but remember to initialize them in `NewDynamoServer()` if the members need initialization.

## Dynamo Client
An RPC client is in the file `Dynamo_Client.go`.

## Utility Functions
A couple utility functions have been provided in the file `Dynamo_Utils.go`. These functions may be helpful when you are writing your code. Feel free to add more functions as you need to this file.

## Setup
You will need to setup your runtime environment variables so that you can build your code and also use the executables that will be generated.
1. If you are using a Mac, open `~/.bash_profile` or if you are using a unix/linux machine, open `~/.bashrc`. Then add the following:
```
export GOPATH=<path to starter code>
export PATH=$PATH:$GOPATH/bin
```
2. Run `source ~/.bash_profile` or `source ~/.bashrc`

## Usage
### Building the code
To build the code, run 
```
./build.sh
```
This should generate `bin/DynamoCoordinator` and `bin/DynamoClient`

### Running the code
To start up a set of nodes, run
```
./run-server.sh [config file]
```
where `config file` is a .ini file.
To run your server in the background, you can use
```
nohup ./run-server.sh [config file] &
```
This will start your server in the background and append the output to a file called `nohup.out`

To run your client, run
```
./run-client.sh
```

### Unit Testing
To test your code, navigate to `src/mydynamotest/` and run
```
go test
```

To run a specific test in your code, run

```
go test -run [testname]
```
**Keep in mind that although `go test` recompiles your testing files, it does not recompile all of your code. If you made modifications to any file in `src/mydynamo`, you will have to run `build.sh` again**

For more information on testing, visit the [Go documentation](https://golang.org/pkg/testing/)
