package mydynamo

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
	"fmt"
)

type DynamoServer struct {
	/*------------Dynamo-specific-------------*/
	wValue         int          //Number of nodes to write to on each Put
	rValue         int          //Number of nodes to read from on each Get
	preferenceList []DynamoNode //Ordered list of other Dynamo nodes to perform operations o
	selfNode       DynamoNode   //This node's address and port info
	nodeID         string       //ID of this node
	store 			map[string][]ObjectEntry	 // The key/value store for this node
	crashUntil		time.Time // simulate node being offline until this moment in time
	gossiper			map[int]Gossiper // map node index from preferenceList to a Gossiper struct
	pListLoc			int // location of this node inside its own preferenceList

}

func (s *DynamoServer) SendPreferenceList(incomingList []DynamoNode, _ *Empty) error {
	s.preferenceList = incomingList
	for i, node := range s.preferenceList {
		if node.Equals(s.selfNode) {
			s.pListLoc	= i
			break
		}
	}
	s.newGossiperMap()
	return nil
}

// Forces server to gossip
// As this method takes no arguments, we must use the Empty placeholder
func (s *DynamoServer) Gossip(_ Empty, _ *Empty) error {
	if s.isCrashed() {
		return fmt.Errorf("server %v is currently offline", s.nodeID)
	}


	conns	:= s.connectToPreferenceNodes()
	for key, _ := range s.store {
		idx	:= 0 // track index for lists of nodes excluding self
		for i, _ := range s.preferenceList {
			// check if current node in preferenceList is self
			if !skipNode(s.pListLoc, i) {
				// ckeck if node needs replications
				if g, ok := s.gossiper[i]; ok {
					// check if need to replicate entries at key
					if entries, ok := g.gossipMap[key]; ok {
						// go through list of entries that need to replicate
						for _, entry := range entries {
							var result *bool
							args	:= NewPutArgs(key, entry.Context, entry.Value)
							if err	:= conns[idx].Call("MyDynamo.PutOnce", args, result); err != nil {
								// There are still some entries to be consumed
								break
							} else {
								g.ConsumeEntry(key)
							}
						}
					}
				}
				idx++
			}
		}
	}
	return nil
}

//Makes server unavailable for some seconds
func (s *DynamoServer) Crash(seconds int, success *bool) error {
	if s.isCrashed() {
		return fmt.Errorf("server %v is currently offline\n", s.nodeID)
	}
	s.crashUntil	= time.Now().Add(time.Second * time.Duration(seconds))
	*success	= true
	return nil
}

// Put a file to this server and W other servers
func (s *DynamoServer) Put(value PutArgs, result *bool) error {

	if s.isCrashed() {
		return fmt.Errorf("server %v is currently offline", s.nodeID)
	}
	/*conn, err	:= rpc.DialHTTP("tcp", s.selfNode.Address + s.selfNode.Port)
	if err != nil {
		return err
	}
	defer conn.Close()

	err	= conn.Call("MyDynamo.PutOnce", value, result)
	return err*/
	value.Context.Clock.Increment(s.nodeID)
	err	:= s.PutOnce(value, result)
	if err != nil {
		return err
	}
	for i, _ := range s.preferenceList {
		if !skipNode(s.pListLoc, i) {
			s.gossiper[i].Append(value.Key, NewObjectEntry(value.Context, value.Value))
		}
	}
	//s.gossiper.Append(value.Key, NewObjectEntry(value.Context, value.Value))

	return nil

}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) Get(key string, result *DynamoResult) error {

	if s.isCrashed() {
		return fmt.Errorf("server %v is currently offline\n", s.nodeID)
	}
/*
	conn, err	:= rpc.DialHTTP("tcp", s.selfNode.Address + s.selfNode.Port)
	if err != nil {
		return err
	}
	defer conn.Close()

	err	= conn.Call("MyDynamo.GetOnce", key, result)
	return err */
	return s.GetOnce(key, result)

}

func (s *DynamoServer) PutOnce(value PutArgs, result *bool) error {
	if s.isCrashed() {
		return fmt.Errorf("server %v is currently offline\n", s.nodeID)
	}
	// Get the list of stored object entries associated with the given key
	storedEntries, ok	:= s.store[value.Key]
	// Check if the key was already present in the store
	if !ok {
		// create new list of object entries and add the passed in entry to the list
		entries	:= make([]ObjectEntry, 0)
		entries	= append(entries, ObjectEntry{
			Context: value.Context,
			Value:	value.Value,
		})
		// associated the newly created list of object entries with the passed in key
		s.store[value.Key]	= entries
		// indicate success
		*result	= true
		return nil
	}

	// new object entry constructed from the given arguments
	newEntry	:= ObjectEntry {
		Context: value.Context,
		Value: value.Value,
	}
	added	:= false	// flag to check if new entry has already been added to list
	concurrent	:= false// flag to check if new entry was concurrent with any concurrent entries
	/*for idx, entry := range storedEntries {
		if newEntry.Context.Clock.LessThan(entry.Context.Clock) {
			*result	= false
			return nil
		}
		// check if new object entry should replaced the currently stored object entry
		if entry.Context.Clock.LessThan(newEntry.Context.Clock) {
			storedEntries[idx]	= newEntry
			added	= true
		}
		if entry.Context.Clock.Concurrent(newEntry.Context.Clock) {
			concurrent	= true
		}
	}*/
	if err := addToEntries(&storedEntries, newEntry, &added, &concurrent); err != nil {
		*result	= false
		return nil
	}

	if added {
		s.store[value.Key]	= storedEntries
		*result	= true
		return nil
	}

	if concurrent {
		s.store[value.Key]	= append(s.store[value.Key], newEntry)
		*result	= true
	} else {
		*result	= false
	}

	return nil
}

func (s *DynamoServer) GetOnce(key string, result *DynamoResult) error {
	if s.isCrashed() {
		return fmt.Errorf("server %v is currently offline\n", s.nodeID)
	}
	var r DynamoResult
	if entries, ok	:= s.store[key]; ok {
		r.EntryList	= entries
	}
	*result	= r

	return nil
}

func (s *DynamoServer) isCrashed() bool {
	return !time.Now().After(s.crashUntil)
}

/* Belows are functions that implement server boot up and initialization */
func NewDynamoServer(w int, r int, hostAddr string, hostPort string, id string) DynamoServer {
	preferenceList := make([]DynamoNode, 0)
	selfNodeInfo := DynamoNode{
		Address: hostAddr,
		Port:    hostPort,
	}
	selfStore	:= make(map[string][]ObjectEntry)
	gossiper	:= make(map[int]Gossiper)
	return DynamoServer{
		wValue:         w,
		rValue:         r,
		preferenceList: preferenceList,
		selfNode:       selfNodeInfo,
		nodeID:         id,
		store:			 selfStore,
		crashUntil:		 time.Time{},
		gossiper:		 gossiper,
		pListLoc:		 -1,
	}
}

func ServeDynamoServer(dynamoServer DynamoServer) error {
	rpcServer := rpc.NewServer()
	e := rpcServer.RegisterName("MyDynamo", &dynamoServer)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Name Registration")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Registered the RPC Interfaces")

	l, e := net.Listen("tcp", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Port Listening")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Listening to Target Port ", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	log.Println(DYNAMO_SERVER, "Serving Server Now")

	return http.Serve(l, rpcServer)
}
