package mydynamotest

import (
   "testing"
   "mydynamo"
   "time"
)

func TestGossiper(t *testing.T) {
   t.Logf("Starting Gossiper test")
   gossiper := mydynamo.NewGossiper()

   entry := mydynamo.NewObjectEntry(
               mydynamo.NewContext(mydynamo.NewVectorClock()), []byte("abcde"))

   // test appending entry to empty list
   gossiper.Append("s1", entry)
   gossipList  := gossiper.GetGossipList("s1")
   if len(gossipList) != 1 {
      t.Errorf("TestGossiper: failed to append entry")
   }
   if !gossipList[0].Context.Clock.Equals(mydynamo.NewVectorClock()) {
      t.Errorf("TestGossiper: incorrect vector clock")
   }
   if !valuesEqual(gossipList[0].Value, []byte("abcde")) {
      t.Errorf("TestGossiper: stored incorrect value")
   }

   // test consuming entry from list with one entry
   gossiper.ConsumeEntry("s1")
   gossipList  = gossiper.GetGossipList("s1")
   if gossipList != nil {
      t.Errorf("TestGossiper: failed to remove from gossipMap after list at key became empty")
   }

   // test adding two entries at same key with entry1.Context < entry2.Context
   gossiper.Append("s1", entry)
   gossipList  = gossiper.GetGossipList("s1")
   if len(gossipList) != 1 {
      t.Errorf("TestGossiper: failed to append entry")
   }
   if !gossipList[0].Context.Clock.Equals(mydynamo.NewVectorClock()) {
      t.Errorf("TestGossiper: incorrect vector clock")
   }
   if !valuesEqual(gossipList[0].Value, []byte("abcde")) {
      t.Errorf("TestGossiper: stored incorrect value")
   }
   clock1  := mydynamo.NewVectorClock()
   clock1.Increment("0")
   entry2 := mydynamo.NewObjectEntry(mydynamo.NewContext(clock1), []byte("bcdef"))
   gossiper.Append("s1", entry2)
   gossipList  = gossiper.GetGossipList("s1")
   if len(gossipList) != 1 {
      t.Errorf("TestGossiper: failed to append entry")
   }
   if !gossipList[0].Context.Clock.Equals(clock1) {
      t.Errorf("TestGossiper: incorrect vector clock")
   }
   if !valuesEqual(gossipList[0].Value, []byte("bcdef")) {
      t.Errorf("TestGossiper: stored incorrect value")
   }

   // add concurrent entry to gossip list
   clock2 := mydynamo.NewVectorClock()
   clock2.Increment("1")
   entry = mydynamo.NewObjectEntry(mydynamo.NewContext(clock2), []byte("xyz"))
   gossiper.Append("s1", entry)
   gossipList  = gossiper.GetGossipList("s1")
   if len(gossipList) != 2 {
      t.Errorf("TestGossiper: failed to append concurrent entry")
   }
   if !gossipList[0].Context.Clock.Equals(clock1) {
      t.Errorf("TestGossiper: incorrect first vector clock")
   }
   if !gossipList[1].Context.Clock.Equals(clock2) {
      t.Errorf("TestGossiper: incorrect second vector clock")
   }
   if !valuesEqual(gossipList[0].Value, []byte("bcdef")) {
      t.Errorf("TestGossiper: stored incorrect first value")
   }
   if !valuesEqual(gossipList[1].Value, []byte("xyz")) {
      t.Errorf("TestGossiper: stored incorrect second value")
   }

   // consume the first of the two entries in the list located at "s1"
   gossiper.ConsumeEntry("s1")
   gossipList  = gossiper.GetGossipList("s1")
   if len(gossipList) != 1 {
      t.Errorf("TestGossiper: failed to append entry")
   }
   if !gossipList[0].Context.Clock.Equals(clock2) {
      t.Errorf("TestGossiper: incorrect vector clock")
   }
   if !valuesEqual(gossipList[0].Value, []byte("xyz")) {
      t.Errorf("TestGossiper: stored incorrect value")
   }
}

func TestBasicGossip(t *testing.T) {
   t.Logf("Starting basic Gossip test")

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

   //Put a value on key "s1"
	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicGossip: client 0 returned nil")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestBasicGossip: client 0 failed to get value")
	}

	if !checkVersionFromResult(gotValue, "0", 1) {
		t.Fail()
		t.Logf("TestBasicGossip: client 0 failed to increment vector clock")
	}

   // gossip to instance 1
   clientInstance0.Gossip()

   //Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr = clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicGossip: client 1 returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestBasicGossip: client 1 failed to get value")
	}

	if !checkVersionFromResult(gotValue, "0", 1) {
		t.Fail()
		t.Logf("TestBasicGossip: client 1 incorrect vector clock")
	}

   // put to node 1 then gossip
   //Put a value on key "s1"
	clientInstance1.Put(PutFreshContext("s2", []byte("xyz")))

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr = clientInstance1.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicGossip: client 1 returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("xyz")) {
		t.Fail()
		t.Logf("TestBasicGossip: client 1 failed to get value")
	}

	if !checkVersionFromResult(gotValue, "1", 1) {
		t.Fail()
		t.Logf("TestBasicGossip: client 1 failed to increment vector clock")
	}

   clientInstance1.Gossip()

   //Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr = clientInstance0.Get("s2")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicGossip: client 0 returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("xyz")) {
		t.Fail()
		t.Logf("TestBasicGossip: client 0 failed to get value")
	}

	if !checkVersionFromResult(gotValue, "1", 1) {
		t.Fail()
		t.Logf("TestBasicGossip: client 0 incorrect vector clock")
	}
}

func TestAllNodesGossip(t *testing.T) {
   t.Logf("Start all nodes Gossip test")

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
   clientInstance3 := MakeConnectedClient(8083)
   clientInstance4 := MakeConnectedClient(8084)

   //Put a value on key "s1"
	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestAllNodesGossip: client 0 returned nil")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestAllNodesGossip: client 0 failed to get value")
	}

	if !checkVersionFromResult(gotValue, "0", 1) {
		t.Fail()
		t.Logf("TestAllNodesGossip: client 0 failed to increment vector clock")
	}

   // gossip to instance 1
   clientInstance0.Gossip()

   // get from node 1
   gotValuePtr = clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestAllNodesGossip: client 1 returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestAllNodesGossip: client 1 failed to get value")
	}

	if !checkVersionFromResult(gotValue, "0", 1) {
		t.Fail()
		t.Logf("TestAllNodesGossip: client 1 incorrect vector clock")
	}
   // get from node 2
   gotValuePtr = clientInstance2.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestAllNodesGossip: client 2 returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestAllNodesGossip: client 2 failed to get value")
	}

	if !checkVersionFromResult(gotValue, "0", 1) {
		t.Fail()
		t.Logf("TestAllNodesGossip: client 2 incorrect vector clock")
	}
   // get from node 3
   gotValuePtr = clientInstance3.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestAllNodesGossip: client 3 returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestAllNodesGossip: client 3 failed to get value")
	}

	if !checkVersionFromResult(gotValue, "0", 1) {
		t.Fail()
		t.Logf("TestAllNodesGossip: client 3 incorrect vector clock")
	}
   // get from node 4
   gotValuePtr = clientInstance4.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestAllNodesGossip: client 4 returned nil")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestAllNodesGossip: client 4 failed to get value")
	}

	if !checkVersionFromResult(gotValue, "0", 1) {
		t.Fail()
		t.Logf("TestAllNodesGossip: client 4 incorrect vector clock")
	}
}
