package mydynamotest

import (
	"mydynamo"
	"testing"
	"math/rand"
	"strconv"
	"time"
)

func TestBasicVectorClock(t *testing.T) {
	t.Logf("Starting TestBasicVectorClock")

	setClusterSize("./myconfig.ini")

	//create two vector clocks
	clock1 := mydynamo.NewVectorClock()
	clock2 := mydynamo.NewVectorClock()

	//Test for equality
	if !clock1.Equals(clock2) {
		t.Fail()
		t.Logf("Vector Clocks were not equal")
	}

	// make a change
	clock1.Increment("0")
	if clock1.Equals(clock2) {
		t.Errorf("Vector clock was changed but still equals other clock")
	}

	// make them equal again
	clock2.Increment("0")
	if !clock2.Equals(clock1) {
		t.Errorf("Vector Clocks were not equal")
	}

	// make changes in both clocks
	setElementVersion(&clock1, "1", 2)
	setElementVersion(&clock2, "1", 3)
	if clock1.Equals(clock2) {
		t.Errorf("Both clocks were changed with different values but were still evaluated as equal")
	}

}

func TestIncrementVectorClock(t *testing.T) {
	setClusterSize("./myconfig.ini")

	clock1	:= mydynamo.NewVectorClock()

	// increment value
	clock1.Increment("0")

	// test value
	if !clock1.VersionIs("0", 1) {
		t.Errorf("Error incrementing element in vector clock")
	}

	// increment again
	clock1.Increment("0")

	// test value
	if !clock1.VersionIs("0", 2) {
		t.Errorf("Error incrementing element in vector clock")
	}

	// increment a different element 4 times
	times	:= 4
	for i := 0; i < times; i++ {
		clock1.Increment("2")
	}

	// test value
	if !clock1.VersionIs("2", times) {
		t.Errorf("Error incrementing element multiple times")
	}
}

func TestLessThanVectorClock(t *testing.T) {
	// "!<" inside error print statements represents "not less than"

	setClusterSize("./myconfig.ini")

	//create two vector clocks
	clock1 := mydynamo.NewVectorClock()
	clock2 := mydynamo.NewVectorClock()

	// test that clocks are not less than each other in both directions
	if clock1.LessThan(clock2) {
		t.Errorf("test: clock1 < clock2; result: %s < %s", mydynamo.PrintFormatVectorClock(clock1), mydynamo.PrintFormatVectorClock(clock2))
	}
	if clock2.LessThan(clock1) {
		t.Errorf("test: clock2 < clock1; result: %s < %s", mydynamo.PrintFormatVectorClock(clock2), mydynamo.PrintFormatVectorClock(clock1))
	}

	// Increment one of the nodes
	clock1.Increment("0")

	// check both directions of the less than test
	if clock1.LessThan(clock2) {
		t.Errorf("test: clock1 < clock2; result: %s < %s", mydynamo.PrintFormatVectorClock(clock1), mydynamo.PrintFormatVectorClock(clock2))
	}
	if !clock2.LessThan(clock1) {
		t.Errorf("test: !(clock2 < clock1); result: %s !< %s", mydynamo.PrintFormatVectorClock(clock2), mydynamo.PrintFormatVectorClock(clock1))
	}

	// increment other clock to make them equal to each other again
	clock2.Increment("0")
	if clock1.LessThan(clock2) {
		t.Errorf("test: clock1 < clock2; result: %s < %s", mydynamo.PrintFormatVectorClock(clock1), mydynamo.PrintFormatVectorClock(clock2))
	}
	if clock2.LessThan(clock1) {
		t.Errorf("test: clock2 < clock1; result: %s < %s", mydynamo.PrintFormatVectorClock(clock2), mydynamo.PrintFormatVectorClock(clock1))
	}

	// set clock1 to [("0", 1), ("1", 2), ("2", 3)]
	// set clock2 to [("0", 1), ("1", 2), ("2", 4)]
	setElementVersion(&clock1, "1", 2)
	setElementVersion(&clock2, "1", 2)
	setElementVersion(&clock1, "2", 3)
	setElementVersion(&clock2, "2", 4)
	if !clock1.LessThan(clock2) {
		t.Errorf("test: !(clock1 < clock2); result: %s !< %s", mydynamo.PrintFormatVectorClock(clock1), mydynamo.PrintFormatVectorClock(clock2))
	}
	if clock2.LessThan(clock1) {
		t.Errorf("test: clock2 < clock1; result: %s < %s", mydynamo.PrintFormatVectorClock(clock2), mydynamo.PrintFormatVectorClock(clock1))
	}

	// set clock1 to [("0", 1), ("1", 2), ("2", 5)]
	setElementVersion(&clock1, "2", 5)
	if clock1.LessThan(clock2) {
		t.Errorf("test: clock1 < clock2; result: %s < %s", mydynamo.PrintFormatVectorClock(clock1), mydynamo.PrintFormatVectorClock(clock2))
	}
	if !clock2.LessThan(clock1) {
		t.Errorf("test: !(clock2 < clock1); result: %s !< %s", mydynamo.PrintFormatVectorClock(clock2), mydynamo.PrintFormatVectorClock(clock1))
	}

	// increment different element of clock2
	// neither clock should be less than the other now
	clock2.Increment("3")
	if clock1.LessThan(clock2) {
		t.Errorf("test: clock1 < clock2; result: %s < %s", mydynamo.PrintFormatVectorClock(clock1), mydynamo.PrintFormatVectorClock(clock2))
	}
	if clock2.LessThan(clock1) {
		t.Errorf("test: clock2 < clock1; result: %s < %s", mydynamo.PrintFormatVectorClock(clock2), mydynamo.PrintFormatVectorClock(clock1))
	}
}

func TestConcurrentVectorClock(t *testing.T) {
	setClusterSize("./myconfig.ini")

	//create two vector clocks
	clock1 := mydynamo.NewVectorClock()
	clock2 := mydynamo.NewVectorClock()

	if clock1.Concurrent(clock2) {
		t.Errorf("test: clock1 || clock2; result: %v || %v", mydynamo.PrintFormatVectorClock(clock1), mydynamo.PrintFormatVectorClock(clock2))
	}
	if clock2.Concurrent(clock1) {
		t.Errorf("test: clock2 || clock1; result: %v || %v", mydynamo.PrintFormatVectorClock(clock2), mydynamo.PrintFormatVectorClock(clock1))
	}

	// increment a version in clock1
	clock1.Increment("0")
	// still not concurrent
	if clock1.Concurrent(clock2) || clock2.Concurrent(clock1) {
		t.Errorf("test: clock1 || clock2; result: %v || %v", mydynamo.PrintFormatVectorClock(clock1), mydynamo.PrintFormatVectorClock(clock2))
	}

	// increment different version in clock2
	clock2.Increment("1")
	// test for concurrency
	if !clock1.Concurrent(clock2) || !clock2.Concurrent(clock1) {
		t.Errorf("test: clock1 || clock2; result: %v || %v", mydynamo.PrintFormatVectorClock(clock1), mydynamo.PrintFormatVectorClock(clock2))
	}

	// make it so they are no longer concurrent
	clock1.Increment("1")
	if clock1.Concurrent(clock2) || clock2.Concurrent(clock1) {
		t.Errorf("test: no longer concurrent")
	}
}

func TestCombineVectorClock(t *testing.T) {
	setClusterSize("./myconfig.ini")

	// following tests use hardcoded values assuming cluster size of at least 5
	if mydynamo.GetClusterSize() > 4 {
		numClocks	:= 3
		clocks	:= makeClocksList(numClocks)
		// what we expect the combined clock to be
		expectedClock	:= mydynamo.NewVectorClock()

		c	:= new(mydynamo.VectorClock)
		c.Combine(clocks)
		if !expectedClock.Equals(*c) {
			t.Logf("test failed: combined newly created vector clocks")
			t.Logf("result:   %s", mydynamo.PrintFormatVectorClock(*c))
			t.Logf("expected: %s", mydynamo.PrintFormatVectorClock(expectedClock))
			t.Fail()
		}

		// make one change to one of the clocks inside of clocks
		clocks[1].Increment("1")

		// set value for expected clock
		expectedClock.Increment("1")

		c	= new(mydynamo.VectorClock)
		c.Combine(clocks)
		if !expectedClock.Equals(*c) {
			t.Logf("test failed: newly created vector clocks combined with a clock" +
						" with one changed (node, version) pair")
			t.Logf("result:   %s", mydynamo.PrintFormatVectorClock(*c))
			t.Logf("expected: %s", mydynamo.PrintFormatVectorClock(expectedClock))
			t.Fail()
		}

		// make a change to a different clock inside of clocks
		incElementVersion(&clocks[2], "2", 2)

		// set the value for what we expect combine to do
		setElementVersion(&expectedClock, "2", 2)

		c	= new(mydynamo.VectorClock)
		c.Combine(clocks)
		if !expectedClock.Equals(*c) {
			t.Logf("test failed: newly created vector clocks combined with two clocks that have different changed nodes")
			t.Logf("result:   %s", mydynamo.PrintFormatVectorClock(*c))
			t.Logf("expected: %s", mydynamo.PrintFormatVectorClock(expectedClock))
			t.Fail()
		}

		// make change so that two clocks inside of clocks have conflicting versions
		setElementVersion(&clocks[2], "2", 3)

		// set the expected value
		expectedClock.Increment("2")

		c	= new(mydynamo.VectorClock)
		c.Combine(clocks)
		if !expectedClock.Equals(*c) {
			t.Logf("test failed: two clocks inside of clocks have conflicting versions")
			t.Logf("result:   %s", mydynamo.PrintFormatVectorClock(*c))
			t.Logf("expected: %s", mydynamo.PrintFormatVectorClock(expectedClock))
			t.Fail()
		}

		// make different changes to the same nodeid in different clocks inside of clocks
		setElementVersion(&clocks[0], "4", 2)
		setElementVersion(&clocks[1], "4", 1)
		setElementVersion(&clocks[2], "4", 3)

		// set the expected value
		setElementVersion(&expectedClock, "4", 3)

		c	= new(mydynamo.VectorClock)
		c.Combine(clocks)
		if !expectedClock.Equals(*c) {
			t.Logf("test failed: three clocks contain different values for same nodeid")
			t.Logf("result:   %s", mydynamo.PrintFormatVectorClock(*c))
			t.Logf("expected: %s", mydynamo.PrintFormatVectorClock(expectedClock))
			t.Fail()
		}

		// make changes so that clocks have same non-zero version for the same nodeid
		// inside of clocks
		setElementVersion(&clocks[0], "3", 3)
		setElementVersion(&clocks[1], "3", 3)
		setElementVersion(&clocks[2], "3", 3)

		// set the expected value
		setElementVersion(&expectedClock, "3", 3)

		c	= new(mydynamo.VectorClock)
		c.Combine(clocks)
		if !expectedClock.Equals(*c) {
			t.Logf("test failed: three clocks contain same non-zero value for same nodeid")
			t.Logf("result:   %s", mydynamo.PrintFormatVectorClock(*c))
			t.Logf("expected: %s", mydynamo.PrintFormatVectorClock(expectedClock))
			t.Fail()
		}
	}

	// fields to tune randomized tests
	maxID	:= mydynamo.GetClusterSize()
	maxClocks	:= 100 // soft max, hard max is maxClocks + minClocks
	minClocks	:= 1
	maxUpdates	:= 10000 // soft max, hard max is maxUpdates + minUpdates
	minUpdates	:= 1
	maxIterations	:= 1000 // soft max, hard max is maxIterations + minIterations
	minIterations	:= 10

	// randomized tests
	rand.Seed(time.Now().UnixNano())
	// random number of tests to perform
	iterations	:= rand.Intn(maxIterations) + minIterations
	for i := 0; i < iterations; i++ {
		// random number of clocks to test with
		numClocks	:= rand.Intn(maxClocks) + minClocks
		// random number of updates to perform
		numUpdates	:= rand.Intn(maxUpdates) + minUpdates

		// get list of random amount of clocks
		clocks	:= makeClocksList(numClocks)

		// reset expected clock
		expectedClock	:= mydynamo.NewVectorClock()

		// make random updates
		for j := 0; j < numUpdates; j++ {
			clockID	:= rand.Intn(numClocks)
			//nodeID	:= strconv.Itoa(rand.Intn(maxID))
			nodeID	:= strconv.Itoa(rand.Intn(maxID))
			clocks[clockID].Increment(nodeID)
			// make sure expectedClock is keeping track of the proper values
			c_ver	:= clocks[clockID].Elements[nodeID]
			if v, ok := expectedClock.Elements[nodeID]; !ok || v < c_ver {
				//expectedClock.Elements[nodeID]	= c_ver
				incElementVersion(&expectedClock, nodeID, c_ver - v)
			}
		} // end of updates

		// test that random updates still combine to the correct clock
		c	:= new(mydynamo.VectorClock)
		c.Combine(clocks)
		if !expectedClock.Equals(*c) {
			t.Logf("iteration %d test failed: randomized updates", i)
			t.Logf("result:   %s", mydynamo.PrintFormatVectorClock(*c))
			t.Logf("expected: %s", mydynamo.PrintFormatVectorClock(expectedClock))
			t.Fail()
		}
	} // end of iterations
}
