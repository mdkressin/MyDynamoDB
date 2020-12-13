package mydynamotest

import (
   "testing"
   "time"
   "log"
   //"mydynamo"
)

func TestBasicCrash(t *testing.T) {
   t.Logf("Starting basic Crash test")
   crashTime   := 0
	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(3 * time.Second)
	<-ready

	setClusterSize("./myconfig.ini")

   //Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	clientInstance := MakeConnectedClient(8080)

   log.Println("start first server crash")
   crashTime   = 3
   result   := clientInstance.Crash(crashTime)
   if !result {
      t.Fail()
      t.Logf("TestBasicCrash: error when crashing server")
   }

   //try Put a value on key "s1"
	result  = clientInstance.Put(PutFreshContext("s1", []byte("abcde")))
   if result {
      t.Fail()
      t.Logf("TestBasicCrash: Put to crashed server did not return an error")
   }

   // try Get
   gotValuePtr := clientInstance.Get("s1")
   if gotValuePtr != nil {
      t.Fail()
      t.Logf("TestBasicCrash: Get to crashed server did not return nil result")
   }

   // try Crash
   result   = clientInstance.Crash(crashTime)
   if result {
      t.Errorf("TestBasicCrash: Crash to crashed server did not return an error")
   }

   // wait for server to come back online
   time.Sleep(time.Duration(crashTime) * time.Second)
   log.Println("first server back online")

   //Put a value on key "s1"
	clientInstance.Put(PutFreshContext("s1", []byte("abcde")))

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr = clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicCrash: Get after server back online returned nil")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestBasicCrash: Get after server back online failed to get value")
	}

	if !checkVersionFromResult(gotValue, "0", 1) {
		t.Fail()
		t.Logf("TestBasicCrash: Failed to increment vector clock")
	}
}

func TestTwoCrash(t *testing.T) {
   t.Logf("Starting Two Crash test")
   crashTime   := 0
	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(3 * time.Second)
	<-ready

	setClusterSize("./myconfig.ini")

   //Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	clientInstance := MakeConnectedClient(8080)

   log.Println("start first server crash")
   crashTime   = 3
   result   := clientInstance.Crash(crashTime)
   if !result {
      t.Fail()
      t.Logf("TestTwoCrash: error when crashing server")
   }

   time.Sleep(time.Second * time.Duration(crashTime))
   log.Println("server back online")

   log.Println("start second server crash")
   result   = clientInstance.Crash(crashTime)
   if !result {
      t.Fail()
      t.Logf("TestTwoCrash: error when crashing server")
   }

   // Check to make sure crash still operates as expected after the second crash
   //try Put a value on key "s1"
	result  = clientInstance.Put(PutFreshContext("s1", []byte("abcde")))
   if result {
      t.Fail()
      t.Logf("TestTwoCrash: Put to crashed server did not return an error")
   }

   // try Get
   gotValuePtr := clientInstance.Get("s1")
   if gotValuePtr != nil {
      t.Fail()
      t.Logf("TestTwoCrash: Get to crashed server did not return nil result")
   }

   // try Crash
   result   = clientInstance.Crash(crashTime)
   if result {
      t.Errorf("TestTwoCrash: Crash to crashed server did not return an error")
   }

   // wait for server to come back online
   time.Sleep(time.Duration(crashTime) * time.Second)
   log.Println("server back online")

   //Put a value on key "s1"
	clientInstance.Put(PutFreshContext("s1", []byte("abcde")))

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr = clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestTwoCrash: Get after server back online returned nil")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestTwoCrash: Get after server back online failed to get value")
	}

	if !checkVersionFromResult(gotValue, "0", 1) {
		t.Fail()
		t.Logf("TestTwoCrash: Failed to increment vector clock")
	}
}

func TestMultipleNodesCrash(t *testing.T) {
   t.Logf("Starting Multiple Nodes Crash test")
   crashTime   := 0
	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(3 * time.Second)
	<-ready

	setClusterSize("./myconfig.ini")

   //Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	clientInstance0 := MakeConnectedClient(8080)
   clientInstance1 := MakeConnectedClient(8081)
   clientInstance2 := MakeConnectedClient(8082)

   // Crash two nodes
   crashTime   = 3
   result   := clientInstance0.Crash(crashTime)
   if !result {
      t.Fail()
      t.Logf("TestMultipleNodesCrash: error when crashing server")
   }
   result   = clientInstance1.Crash(crashTime)
   if !result {
      t.Fail()
      t.Logf("TestMultipleNodesCrash: error when crashing server")
   }

   // Put to crashed nodes
   result  = clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
   if result {
      t.Fail()
      t.Logf("TestMultipleNodesCrash: Put to crashed server did not return an error")
   }
   result  = clientInstance1.Put(PutFreshContext("s1", []byte("abcde")))
   if result {
      t.Fail()
      t.Logf("TestMultipleNodesCrash: Put to crashed server did not return an error")
   }

   // Put and Get from online node
   clientInstance2.Put(PutFreshContext("s1", []byte("abcde")))

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance2.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestMultipleNodesCrash: Get from online node return nil")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestMultipleNodesCrash: Get from online node failed to get value")
	}

	if !checkVersionFromResult(gotValue, "2", 1) {
		t.Fail()
		t.Logf("TestMultipleNodesCrash: Failed to increment vector clock")
	}

}

func TestDurationCrash(t *testing.T) {
   t.Logf("Starting Duration Crash test")
   crashTime   := 0
	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(3 * time.Second)
	<-ready

	setClusterSize("./myconfig.ini")

   //Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	clientInstance0 := MakeConnectedClient(8080)

   // crash the node and then time the duration
   crashTime   = 3
   log.Printf("crashing server for %v seconds\n", crashTime)
   result   := clientInstance0.Crash(crashTime)
   if !result {
      t.Fail()
      t.Logf("TestDurationCrash: error when crashing server")
   }
   time.Sleep(time.Millisecond * time.Duration(crashTime-1)*1000)
   log.Printf("slept for %v seconds\n", crashTime-1)

   // try to put. server should still be offline
   result  = clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
   if result {
      t.Fail()
      t.Logf("TestDurationCrash: Put to crashed server after insufficient wait time did not return an error")
   }
   time.Sleep(time.Second)
   t.Logf("server back online")

   // crash the node and then time the duration more precisely
   crashTime   = 3
   log.Printf("crashing server for %v seconds\n", crashTime)
   result   = clientInstance0.Crash(crashTime)
   if !result {
      t.Fail()
      t.Logf("TestDurationCrash: error when crashing server")
   }
   time.Sleep(time.Millisecond * (time.Duration(crashTime)*1000 - 200))
   log.Printf("slept for %v seconds\n", float32(crashTime)-0.2)

   // try to put. server should still be offline
   result  = clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
   if result {
      t.Fail()
      t.Logf("TestDurationCrash: Put to crashed server after insufficient wait time did not return an error")
   }

   time.Sleep(time.Second)
   // crash the node for a different amount of time and time the duration
   crashTime   = 5
   log.Printf("crashing server for %v seconds\n", crashTime)
   result   = clientInstance0.Crash(crashTime)
   if !result {
      t.Fail()
      t.Logf("TestDurationCrash: error when crashing server")
   }
   for i := 0; i < crashTime-1; i++ {
      time.Sleep(time.Second)
      log.Printf("slept 1 second")

      // try to put. server should still be offline
      result  = clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
      if result {
         t.Fail()
         t.Logf("TestDurationCrash: Put to crashed server after insufficient wait time did not return an error")
      }
   }
   time.Sleep(time.Second)
   log.Println("slept 1 second")
   t.Logf("server back online")

   //Put a value on key "s1"
	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestDurationCrash: Get after server back online returned nil")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestDurationCrash: Get after server back online failed to get value")
	}

	if !checkVersionFromResult(gotValue, "0", 1) {
		t.Fail()
		t.Logf("TestDurationCrash: Failed to increment vector clock")
	}
}

func TestMultipleNodesDifferentDurationCrash(t *testing.T) {
   t.Logf("Starting Multiple Nodes Different Duration Crash test")
   crashTime   := 0
	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(3 * time.Second)
	<-ready

	setClusterSize("./myconfig.ini")

   //Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	clientInstance0 := MakeConnectedClient(8080)
   clientInstance1 := MakeConnectedClient(8081)
   clientInstance2 := MakeConnectedClient(8082)

   // crash nodes with different durations
   crashTime   = 2
   result   := clientInstance0.Crash(crashTime)
   if !result {
      t.Fail()
      t.Logf("TestMultipleNodesDifferentDurationCrash: error when crashing server")
   }
   result   = clientInstance1.Crash(crashTime*2)
   if !result {
      t.Fail()
      t.Logf("TestMultipleNodesDifferentDurationCrash: error when crashing server")
   }
   result   = clientInstance2.Crash(crashTime*3)
   if !result {
      t.Fail()
      t.Logf("TestMultipleNodesDifferentDurationCrash: error when crashing server")
   }

   // rpc calls to all three nodes should throw errors
   result  = clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
   if result {
      t.Fail()
      t.Logf("TestMultipleNodesDifferentDurationCrash: Put to crashed server did not return an error")
   }
   result  = clientInstance1.Put(PutFreshContext("s1", []byte("abcde")))
   if result {
      t.Fail()
      t.Logf("TestMultipleNodesDifferentDurationCrash: Put to crashed server did not return an error")
   }
   result  = clientInstance2.Put(PutFreshContext("s1", []byte("abcde")))
   if result {
      t.Fail()
      t.Logf("TestMultipleNodesDifferentDurationCrash: Put to crashed server did not return an error")
   }

   // sleep for crash time (i.e. node 0 should be back online)
   time.Sleep(time.Second * time.Duration(crashTime))
   // node 0 should work but nodes 1 and 2 should throw errors
   clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestMultipleNodesDifferentDurationCrash: Get after server back online returned nil")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestMultipleNodesDifferentDurationCrash: Get after server back online failed to get value")
	}

	if !checkVersionFromResult(gotValue, "0", 1) {
		t.Fail()
		t.Logf("TestMultipleNodesDifferentDurationCrash: Failed to increment vector clock")
	}
   result  = clientInstance1.Put(PutFreshContext("s1", []byte("abcde")))
   if result {
      t.Fail()
      t.Logf("TestMultipleNodesDifferentDurationCrash: Put to crashed server did not return an error")
   }
   result  = clientInstance2.Put(PutFreshContext("s1", []byte("abcde")))
   if result {
      t.Fail()
      t.Logf("TestMultipleNodesDifferentDurationCrash: Put to crashed server did not return an error")
   }

   // sleep for crash time (i.e. node 1 should be back online)
   time.Sleep(time.Second * time.Duration(crashTime))
   // nodes 0 and 1 should be back online but node 2 should throw errors
   clientInstance1.Put(PutFreshContext("s1", []byte("abcde")))
	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr = clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestMultipleNodesDifferentDurationCrash: Get after server back online returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestMultipleNodesDifferentDurationCrash: Get after server back online failed to get value")
	}

	if !checkVersionFromResult(gotValue, "1", 1) {
		t.Fail()
		t.Logf("TestMultipleNodesDifferentDurationCrash: Failed to increment vector clock")
	}
   result  = clientInstance2.Put(PutFreshContext("s1", []byte("abcde")))
   if result {
      t.Fail()
      t.Logf("TestMultipleNodesDifferentDurationCrash: Put to crashed server did not return an error")
   }

   // sleep for crash time (i.e. node 2 should be back online)
   time.Sleep(time.Second * time.Duration(crashTime))
   // all nodes should be back online
   clientInstance2.Put(PutFreshContext("s1", []byte("abcde")))
	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr = clientInstance2.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestMultipleNodesDifferentDurationCrash: Get after server back online returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestMultipleNodesDifferentDurationCrash: Get after server back online failed to get value")
	}

	if !checkVersionFromResult(gotValue, "2", 1) {
		t.Fail()
		t.Logf("TestMultipleNodesDifferentDurationCrash: Failed to increment vector clock")
	}
}
