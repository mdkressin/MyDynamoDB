package mydynamo

import (
	"fmt"
	"sync"
	"net/rpc"
)

var numServers	int
//Removes an element at the specified index from a list of ObjectEntry structs
func remove(list []ObjectEntry, index int) []ObjectEntry {
	return append(list[:index], list[index+1:]...)
}

//Returns true if the specified list of ints contains the specified item
func contains(list []int, item int) bool {
	for _, v := range list {
		if v == item {
			return true
		}
	}
	return false
}

//Rotates a preference list by one, so that we can give each node a unique preference list
func RotateServerList(list []DynamoNode) []DynamoNode {
	return append(list[1:], list[0])
}

//Creates a new Context with the specified Vector Clock
func NewContext(vClock VectorClock) Context {
	return Context{
		Clock: vClock,
	}
}

//Creates a new PutArgs struct with the specified members.
func NewPutArgs(key string, context Context, value []byte) PutArgs {
	return PutArgs{
		Key:     key,
		Context: context,
		Value:   value,
	}
}

//Creates a new DynamoNode struct with the specified members
func NewDynamoNode(addr string, port string) DynamoNode {
	return DynamoNode{
		Address: addr,
		Port:    port,
	}
}

func NewObjectEntry(context Context, value []byte) ObjectEntry {
	return ObjectEntry{
		Context:	context,
		Value:	value,
	}
}

func NewGossiper() Gossiper {
	g	:= make(map[string][]ObjectEntry)
	var m sync.Mutex
	return Gossiper{
		gossipMap:	g,
		m:				m,
	}
}

func SetClusterSize(size int) {
	numServers	= size
}
func GetClusterSize() int {
	return numServers
}
func PrintFormatVectorClock(clock VectorClock) string {
	format	:= "["
	for i, ver	:= range clock.Elements {
		format	+= fmt.Sprintf("(id %v: ver %v),", i, ver)
	}
	format	= format[:len(format)-1] + "]"
	return format
}

func (g Gossiper) Append(key string, newEntry ObjectEntry) {
	g.m.Lock()

	if storedEntries, ok := g.gossipMap[key]; !ok {
		g.gossipMap[key]	= []ObjectEntry{newEntry}
	} else {
		added	:= false
		concurrent	:= false
		if err := addToEntries(&storedEntries, newEntry, &added, &concurrent); err == nil {
			if added {
				g.gossipMap[key]	= storedEntries
			} else if concurrent {
				g.gossipMap[key]	= append(g.gossipMap[key], newEntry)
			}
		}
	}

	g.m.Unlock()
}

func (g Gossiper) ConsumeEntry(key string)  {
	g.m.Lock()

//	entry	= g.gossipMap[0]
//	g.gossipMap	= remove(g.gossipMap, 0)
	g.gossipMap[key]	= remove(g.gossipMap[key], 0)
	if len(g.gossipMap[key]) == 0 {
		delete(g.gossipMap, key)
	}

	g.m.Unlock()
//	return entry
}

/*
	Opens RPC connections to the other nodes inside of this node's preferenceLIst
*/
func (s DynamoServer) connectToPreferenceNodes() []*rpc.Client {
	conns	:= make([]*rpc.Client, 0)
	for i, node	:= range s.preferenceList {
		if !skipNode(s.pListLoc, i) {
			conn, err	:= rpc.DialHTTP("tcp", node.Address + ":" + node.Port)
			if err != nil {
				fmt.Println(err)
			} else {
				conns	= append(conns, conn)
			}
		}
	}
	return conns
}

func closeConnections(conns []rpc.Client) {
	for _, conn := range conns {
		if err := conn.Close(); err != nil {
			fmt.Println(err)
		}
	}
}

func (s DynamoNode) Equals(otherNode DynamoNode) bool {
	return s.Address	== otherNode.Address &&
				s.Port	== otherNode.Port
}

func (s *DynamoServer) newGossiperMap() {
	for idx, _ := range s.preferenceList {
		if !skipNode(s.pListLoc, idx) {
			s.gossiper[idx]	= NewGossiper()
		}
	}
}

func addToEntries(entries *[]ObjectEntry, newEntry ObjectEntry, add, concurrent *bool) error {
	idx	:= 0
	newEntries := *entries
	for _, entry := range *entries {
		// Check causality

		if newEntry.Context.Clock.LessThan(entry.Context.Clock) {
			return fmt.Errorf("newContext < oldContext")
		}
		// check if new object entry should replaced the currently stored object entry
		if entry.Context.Clock.LessThan(newEntry.Context.Clock) {
			//(*entries)[idx]	= newEntry
			newEntries	= remove(newEntries, idx)
			*add	= true
		} else {
			idx++
		}
		if entry.Context.Clock.Concurrent(newEntry.Context.Clock) {
			*concurrent	= true
		}
	}
	if *add {
		newEntries	= append(newEntries, newEntry)
		*entries	= newEntries
	}
	return nil
}

// Determine whether the current node in a servers's preferenceList should be skipped
func skipNode(selfNode, otherNode int) bool {
	return otherNode == selfNode || selfNode == -1
}

func (g Gossiper) GetGossipList(key string) []ObjectEntry {
	return g.gossipMap[key]
}

func RemoveResultAncestors(result *DynamoResult) {
	idx := 0
	entryList	:= make([]ObjectEntry, len(result.EntryList))
	copy(entryList, result.EntryList)

	for _, entry := range result.EntryList {
		hasDescendant	:= false
		for _, possible_descendant := range entryList {
			if entry.Context.Clock.LessThan(possible_descendant.Context.Clock) {
				hasDescendant	= true
				break
			}
		}
		if hasDescendant {
			entryList	= remove(entryList, idx)
		} else {
			idx++
		}
	}
	result.EntryList	= entryList
}

func (s DynamoServer) printGossiper() {
	fmt.Println("----------START GOSSIPER------------")

	for id, gossiper := range s.gossiper {
		fmt.Printf("node %v: ", id)
		fmt.Println(gossiper.gossipMap)
	}

	fmt.Println("-----------END GOSSIPER-------------")
}
