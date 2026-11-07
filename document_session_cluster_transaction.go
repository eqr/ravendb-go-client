package ravendb

import (
	"context"
	"reflect"
	"strings"
)

const automaticClusterTransactionGuardKeyPrefix = "rvn-atomic/"

type ClusterTransactionOperations struct {
	session                                   *InMemoryDocumentSessionOperations
	state                                     map[string]*CompareExchangeSessionValue
	missingDocumentsToAtomicGuardChangeVector map[string]string
}

func (cto *ClusterTransactionOperations) registerAutomaticGuardIncludes(includes map[string]interface{}) error {
	if cto.session.noTracking {
		return nil
	}
	for includeKey, raw := range includes {
		value, ok := raw.(map[string]interface{})
		if !ok {
			return newIllegalStateError("CompareExchangeValueIncludes contains an invalid value for " + includeKey)
		}
		key := includeKey
		if responseKey, ok := value["Key"].(string); ok && responseKey != "" {
			key = responseKey
		}
		if !strings.HasPrefix(strings.ToLower(key), automaticClusterTransactionGuardKeyPrefix) {
			continue
		}
		changeVector, ok := value["ChangeVector"].(string)
		if !ok || changeVector == "" {
			continue
		}
		if cto.missingDocumentsToAtomicGuardChangeVector == nil {
			cto.missingDocumentsToAtomicGuardChangeVector = make(map[string]string)
		}
		documentID := key[len(automaticClusterTransactionGuardKeyPrefix):]
		cto.missingDocumentsToAtomicGuardChangeVector[strings.ToLower(documentID)] = changeVector
	}
	return nil
}

func (cto *ClusterTransactionOperations) missingAtomicGuardChangeVector(documentID string) (string, bool) {
	changeVector, ok := cto.missingDocumentsToAtomicGuardChangeVector[strings.ToLower(documentID)]
	return changeVector, ok
}

func (cto *ClusterTransactionOperations) GetNumberOfTrackedCompareExchangeValues() int {
	if nil == cto.state {
		return 0
	}

	return len(cto.state)
}

func (cto *ClusterTransactionOperations) IsTracked(key string) bool {
	_, exists := cto.tryGetCompareExchangeValueFromSession(key)
	return exists
}

func (cto *ClusterTransactionOperations) tryGetCompareExchangeValueFromSession(key string) (*CompareExchangeSessionValue, bool) {
	value, exists := cto.state[key]
	return value, exists
}

func (cto *ClusterTransactionOperations) updateState(key string, index int64) (*CompareExchangeSessionValue, bool) {
	value, exists := cto.tryGetCompareExchangeValueFromSession(key)

	if exists == false {
		return nil, false
	}

	value.UpdateState(index)
	return value, true
}

func (cto *ClusterTransactionOperations) Clear() {
	cto.state = make(map[string]*CompareExchangeSessionValue)
	cto.missingDocumentsToAtomicGuardChangeVector = nil
}

func (cto *ClusterTransactionOperations) CreateCompareExchangeValue(key string, item interface{}) (interface{}, error) {
	if len(key) == 0 {
		return nil, newIllegalArgumentError("Key cannot be null or empty")
	}

	var exists bool
	var value *CompareExchangeSessionValue
	value, exists = cto.tryGetCompareExchangeValueFromSession(key)

	if exists == false {
		var err error
		value, err = NewCompareExchangeSessionValue(key, 0, compareExchangeValueStateNone)
		if err != nil {
			return nil, err
		}
		cto.state[key] = value
	}

	return value.Create(item)
}

func (cto *ClusterTransactionOperations) prepareCompareExchangeEntities(result *saveChangesData) error {
	if len(cto.state) == 0 {
		return nil
	}

	for _, value := range cto.state {
		command, err := value.GetCommand(cto.session)
		if err != nil {
			return err
		}
		if command == nil {
			continue
		}

		result.addSessionCommandData(command)
	}

	return nil
}

func (cto *ClusterTransactionOperations) GetCompareExchangeValue(clazz reflect.Type, key string) (*CompareExchangeValue, error) {
	return cto.GetCompareExchangeValueContext(context.Background(), clazz, key)
}

func (cto *ClusterTransactionOperations) GetCompareExchangeValueContext(ctx context.Context, clazz reflect.Type, key string) (*CompareExchangeValue, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	return cto.getCompareExchangeValueInternalContext(ctx, clazz, key)
}

func (cto *ClusterTransactionOperations) GetCompareExchangeValuesWithKeys(clazz reflect.Type, keys []string) (map[string]*CompareExchangeValue, error) {
	return cto.GetCompareExchangeValuesWithKeysContext(context.Background(), clazz, keys)
}

func (cto *ClusterTransactionOperations) GetCompareExchangeValuesWithKeysContext(ctx context.Context, clazz reflect.Type, keys []string) (map[string]*CompareExchangeValue, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	return cto.getCompareExchangeValuesInternalWithKeysContext(ctx, clazz, keys)
}

func (cto *ClusterTransactionOperations) GetCompareExchangeValues(clazz reflect.Type, startsWith string, start int, pageSize int) (map[string]*CompareExchangeValue, error) {
	return cto.GetCompareExchangeValuesContext(context.Background(), clazz, startsWith, start, pageSize)
}

func (cto *ClusterTransactionOperations) GetCompareExchangeValuesContext(ctx context.Context, clazz reflect.Type, startsWith string, start int, pageSize int) (map[string]*CompareExchangeValue, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	return cto.getCompareExchangeValuesInternalContext(ctx, clazz, startsWith, start, pageSize)
}

func (cto *ClusterTransactionOperations) getCompareExchangeValuesInternalContext(ctx context.Context, clazz reflect.Type, startsWith string, start int, pageSize int) (map[string]*CompareExchangeValue, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	_ = cto.session.incrementRequestCount()

	operation, err := NewGetCompareExchangeValuesOperation(clazz, startsWith, start, pageSize)
	if err != nil {
		return nil, err
	}
	if err = cto.session.GetOperations().SendContext(ctx, operation, cto.session.sessionInfo); err != nil {
		return nil, err
	}

	operationResult := operation.Command.Result
	results := make(map[string]*CompareExchangeValue)

	for _, value := range operationResult {
		sessionValue, err := cto.registerCompareExchangeValue(value)
		if err != nil {
			return nil, err
		}
		resultValue, err := sessionValue.GetValue(clazz, cto.session)
		if err != nil {
			return nil, err
		}
		results[value.GetKey()] = resultValue
	}

	return results, nil
}

func (cto *ClusterTransactionOperations) getCompareExchangeValuesInternalWithKeysContext(ctx context.Context, clazz reflect.Type, keys []string) (map[string]*CompareExchangeValue, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	results, notTracked, err := cto.getCompareExchangeValuesWithKeysFromSessionInternal(clazz, keys)
	if err != nil {
		return nil, err
	}

	if notTracked == nil || len(notTracked) == 0 {
		return results, nil
	}

	_ = cto.session.incrementRequestCount()

	operation, err := NewGetCompareExchangeValuesOperationWithKeys(clazz, notTracked)
	if err != nil {
		return nil, err
	}
	if err = cto.session.GetOperations().SendContext(ctx, operation, cto.session.sessionInfo); err != nil {
		return nil, err
	}

	operationResult := operation.Command.Result

	for _, key := range notTracked {
		value, exists := operationResult[key]
		if !exists || value == nil {
			_, _ = cto.registerMissingCompareExchangeValue(key)
			results[key] = nil
			continue
		}

		sessionValue, err := cto.registerCompareExchangeValue(value)
		if err != nil {
			return nil, err
		}
		resultValue, err := sessionValue.GetValue(clazz, cto.session)
		if err != nil {
			return nil, err
		}
		results[value.GetKey()] = resultValue
	}

	return results, nil
}

func (cto *ClusterTransactionOperations) getCompareExchangeValueInternalContext(ctx context.Context, clazz reflect.Type, key string) (*CompareExchangeValue, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	v, notTracked := cto.getCompareExchangeValueFromSessionInternal(clazz, key)
	if !notTracked {
		return v, nil
	}

	_ = cto.session.incrementRequestCount()

	operation, err := NewGetCompareExchangeValueOperation(clazz, key)
	if err != nil {
		return nil, err
	}
	if err = cto.session.GetOperations().SendContext(ctx, operation, cto.session.sessionInfo); err != nil {
		return nil, err
	}

	value := operation.Command.Result
	if value == nil {
		_, _ = cto.registerMissingCompareExchangeValue(key)
		return nil, nil
	}

	sessionValue, err := cto.registerCompareExchangeValue(value)
	if err != nil || sessionValue == nil {
		return nil, err
	}
	return sessionValue.GetValue(clazz, cto.session)
}

func (cto *ClusterTransactionOperations) registerCompareExchangeValue(value *CompareExchangeValue) (*CompareExchangeSessionValue, error) {
	if cto.session.noTracking {
		return NewCompareExchangeSessionValueWithValue(value)
	}

	var err error
	sesionValue, exists := cto.state[value.GetKey()]

	if exists == false || sesionValue == nil {
		sesionValue, err = NewCompareExchangeSessionValueWithValue(value)
		if err != nil {
			return nil, err
		}
		cto.state[value.GetKey()] = sesionValue
		return sesionValue, nil
	}

	err = sesionValue.UpdateValue(value)

	return sesionValue, err
}

func (cto *ClusterTransactionOperations) registerMissingCompareExchangeValue(key string) (*CompareExchangeSessionValue, error) {
	value, err := NewCompareExchangeSessionValue(key, -1, exchangeValueStateMissing)

	if err != nil {
		return nil, err
	}

	if cto.session.noTracking {
		return value, nil
	}

	cto.state[key] = value
	return value, nil
}

func (cto *ClusterTransactionOperations) getCompareExchangeValueFromSessionInternal(clazz reflect.Type, key string) (compareExchangeValue *CompareExchangeValue, notTracked bool) {
	result, exist := cto.tryGetCompareExchangeValueFromSession(key)

	if exist == false {
		return nil, true
	}

	//we've already deserialized, maybe except situation when user wants get deriative type?

	return result.value, false
}

func (cto *ClusterTransactionOperations) getCompareExchangeValuesWithKeysFromSessionInternal(clazz reflect.Type, keys []string) (map[string]*CompareExchangeValue, []string, error) {
	var results map[string]*CompareExchangeValue
	results = make(map[string]*CompareExchangeValue)
	var notTracked []string

	if keys == nil || len(keys) == 0 {
		return results, nil, nil
	}

	for _, key := range keys {
		cev, exists := cto.tryGetCompareExchangeValueFromSession(key)

		if exists {
			val, err := cev.GetValue(clazz, cto.session)
			if err != nil {
				return results, nil, err
			}
			results[key] = val
			continue
		}

		notTracked = append(notTracked, key)
	}

	return results, notTracked, nil
}

func (cto *ClusterTransactionOperations) DeleteCompareExchangeValue(item *CompareExchangeValue) error {
	if item == nil {
		return newIllegalArgumentError("Item cannot be null")
	}

	sessionValue, exists := cto.tryGetCompareExchangeValueFromSession(item.GetKey())
	if exists == false {
		sessionValue, _ = NewCompareExchangeSessionValue(item.GetKey(), 0, compareExchangeValueStateNone)
		cto.state[item.GetKey()] = sessionValue
	}

	sessionValue.Delete(item.GetIndex())
	return nil
}

func (cto *ClusterTransactionOperations) DeleteCompareExchangeValueByKey(key string, index int64) error {
	if len(key) == 0 {
		return newIllegalStateError("Key cannot be null nor empty")
	}

	sessionValue, exists := cto.tryGetCompareExchangeValueFromSession(key)
	if exists == false {
		sessionValue, _ = NewCompareExchangeSessionValue(key, 0, compareExchangeValueStateNone)
		cto.state[key] = sessionValue
	}

	sessionValue.Delete(index)
	return nil
}
