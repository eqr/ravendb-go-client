package ravendb

import (
	"testing"
)

func TestDatabaseChangesMarkConnectedSurvivesReconnect(t *testing.T) {
	c := &DatabaseChanges{chIsConnected: make(chan error, 1)}

	c.markConnected()
	c.markConnected()

	select {
	case err := <-c.chIsConnected:
		if err != nil {
			t.Fatalf("connected signal carried error %v", err)
		}
	default:
		t.Fatal("connected signal not delivered")
	}
}
