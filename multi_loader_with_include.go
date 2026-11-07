package ravendb

import (
	"context"
	"reflect"
)

// ILoaderWithInclude is NewMultiLoaderWithInclude

type MultiLoaderWithInclude struct {
	session  *DocumentSession
	includes []string
}

func NewMultiLoaderWithInclude(session *DocumentSession) *MultiLoaderWithInclude {
	return &MultiLoaderWithInclude{
		session: session,
	}
}

func (l *MultiLoaderWithInclude) Include(path string) *MultiLoaderWithInclude {
	l.includes = append(l.includes, path)
	return l
}

// results should be map[string]*struct
func (l *MultiLoaderWithInclude) LoadMulti(results interface{}, ids []string) error {
	return l.LoadMultiContext(context.Background(), results, ids)
}

func (l *MultiLoaderWithInclude) LoadMultiContext(ctx context.Context, results interface{}, ids []string) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if len(ids) == 0 {
		return newIllegalArgumentError("ids cannot be empty array")
	}
	if err := checkValidLoadMultiArg(results, "results"); err != nil {
		return err
	}
	return l.session.loadInternalMultiContext(ctx, results, ids, l.includes)
}

// TODO: needs a test
// TODO: better implementation
func (l *MultiLoaderWithInclude) Load(result interface{}, id string) error {
	return l.LoadContext(context.Background(), result, id)
}

func (l *MultiLoaderWithInclude) LoadContext(ctx context.Context, result interface{}, id string) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if id == "" {
		return newIllegalArgumentError("id cannot be empty string")
	}
	if err := checkIsPtrPtrStruct(result, "result"); err != nil {
		return err
	}

	rt := reflect.TypeOf(result).Elem()
	mapType := reflect.MapOf(stringType, rt)
	m := reflect.MakeMap(mapType)
	if err := l.session.loadInternalMultiContext(ctx, m.Interface(), []string{id}, l.includes); err != nil {
		return err
	}
	res := m.MapIndex(reflect.ValueOf(id))
	if res.IsNil() {
		return nil
	}
	return setInterfaceToValue(result, res.Interface())
}
