package ravendb

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

type User struct {
	ID   string
	Name string
}

func TestNewDocumentConventionsDefaultsToRoundRobinReads(t *testing.T) {
	assert.Equal(t, ReadBalanceBehaviorRoundRobin, NewDocumentConventions().ReadBalanceBehavior)
}

func TestDefaultGetCollectionName(t *testing.T) {
	name := getCollectionNameForTypeOrEntity(&User{})
	assert.Equal(t, "Users", name)
	name = getCollectionNameForTypeOrEntity(reflect.TypeOf(&User{}))
	assert.Equal(t, "Users", name)
}
