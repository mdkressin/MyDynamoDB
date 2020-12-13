package mydynamo


type VectorClock struct {
	//todo
	Elements	map[string]int
}

type Element struct {
	NodeID	string
	Version	int
}

// map to location of NodeID inside of Elements in vector clocks
var idMap map[string]int

//Creates a new VectorClock
func NewVectorClock() VectorClock {
	elements 	:= make(map[string]int)
	/*idMap	= make(map[string]int)
	for i := 0; i < numServers; i++ {
		k	:= strconv.Itoa(i)
		elements[i]	= NewElement(k)

		idMap[k]	= i
	}
*/
	return VectorClock {
		Elements:	elements,
	}
}

func NewElement(id string) Element {
	return Element{
		NodeID: id,
		Version: 0,
	}
}

//Returns true if the other VectorClock is causally descended from this one
func (s VectorClock) LessThan(otherClock VectorClock) bool {
/*
	for i, elem := range s.Elements {
		o_elem	:= otherClock.Elements[i]
		if elem.Version > o_elem.Version {
			return false
		}
	}*/
	if len(s.Elements) != 0 {
		if len(otherClock.Elements) == 0 {
			return false
		}
		for id, ver := range s.Elements {
			if o_ver, ok	:= otherClock.Elements[id]; !ok || o_ver < ver {
				return false
			}
		}
	}
	// make sure that the clocks are not equal
	return !s.Equals(otherClock)
}

//Returns true if neither VectorClock is causally descended from the other
func (s VectorClock) Concurrent(otherClock VectorClock) bool {

	// Concurrent if a_i < b_i and a_j > b_j for some i,j
	for i, a_i := range s.Elements {
		b_i := otherClock.Elements[i]
		if a_i < b_i {
			for j, b_j := range otherClock.Elements {
				a_j	:= s.Elements[j]
				if a_j > b_j {
					return true
				}
			}
		} else if a_i > b_i {
			for j, b_j := range otherClock.Elements {
				a_j	:= s.Elements[j]
				if a_j < b_j {
					return true
				}
			}
		}
	}
	/*for i, a_i := range s.Elements {
		b_i	:= otherClock.Elements[i]
		if a_i.Version < b_i.Version {
			for j, b_j := range otherClock.Elements {
				a_j	:= s.Elements[j]
				if a_j.Version > b_j.Version {
					return true
				}
			}
		}
	}*/
	return false
}

//Increments this VectorClock at the element associated with nodeId
func (s *VectorClock) Increment(nodeId string) {
	s.Elements[nodeId]++
	//s.Elements[idMap[nodeId]].Version++
}

//Changes this VectorClock to be causally descended from all VectorClocks in clocks
func (s *VectorClock) Combine(clocks []VectorClock) {
	// new clock to be causally descended from all VectorClocks in clocks as
	// well as in s
	newClock	:= NewVectorClock()
	/*elements	:= make([]Element, numServers)
	copy(elements, newClock.Elements)

	// add this clock to the list of clocks
	clocks	= append(clocks, *s)

	for _, clock := range clocks {
		for i, elem := range clock.Elements {
			// keep the version of the element that has the larger version
			if elements[i].Version < elem.Version {
				elements[i].Version	= elem.Version
			}
		}
	}
	copy(newClock.Elements, elements)*/
	elements	:= newClock.Elements

	clocks	= append(clocks, *s)
	for _, clock := range clocks {
		for id, ver := range clock.Elements {
			if v, ok := elements[id]; !ok || v < ver{
				elements[id]	= ver
			}
		}
	}
	*s	= newClock
}

//Tests if two VectorClocks are equal
func (s *VectorClock) Equals(otherClock VectorClock) bool {
	if len(s.Elements) != len(otherClock.Elements) {
		return false
	}
/*
	for i, elem := range s.Elements {
		if !elem.equals(otherClock.Elements[i]) {
			return false
		}
	}*/
	for id, ver := range s.Elements {
		if ver != otherClock.Elements[id] {
			return false
		}
	}
	return true
}

func (s VectorClock) VersionIs(id string, version int) bool {
	//return s.Elements[idMap[id]].Version == version
	return s.Elements[id] == version
}
// GetNodeAsciiToInt
func (s VectorClock) GetNodeAtoi(id string) int {
	return idMap[id]
}
// Test if two Elements are equal
func (e Element) equals(otherElement Element) bool {
	if e.NodeID != otherElement.NodeID {
		return false
	}
	return e.Version == otherElement.Version
}
