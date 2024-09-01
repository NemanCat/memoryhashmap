//unit tests for MemoryHashMap

package memoryhashmap

import (
	"fmt"
	"strconv"
	"testing"
)

var hashmap *PersistentMemoryHashMap
var err error

//тестируем создание PeristentMemoryHashMap
func TestCreatePersistentMemoryHashMap(t *testing.T) {
	hashmap, err = CreatePersistentMemoryHashMap("C:\\Temp", "test.db", "test")
	if err != nil {
		t.Errorf("error creating hashmap")
	}
	if hashmap == nil {
		t.Errorf("got nil hashmap")
	}
}

//тестируем добавление объекта
func TestAddUpdateObject(t *testing.T) {
	got := hashmap.AddUpdateObject("test", []byte("test value"))
	if got != nil {
		t.Errorf("error addind object %s", err.Error())
	}
}

//тестируем получение количества объектов в списке
func TestCount(t *testing.T) {
	got := hashmap.Count()
	if got != 1 {
		t.Errorf("error counting objects")
	}
}

//тестируем получение списка объектов
func TestGetList(t *testing.T) {
	got := hashmap.GetList()
	if (got == nil) || (len(got) == 0) {
		t.Errorf("error getting objects")
	}
}

//тестируем поиск по ключу
func TestFindByKey(t *testing.T) {
	got := hashmap.FindByKey("test")
	if got == nil {
		t.Errorf("error finding object")
	}
}

//тестируем удаление объекта
func TestDeleteObject(t *testing.T) {
	got := hashmap.DeleteObject("test")
	if got != nil {
		t.Errorf("error deleting object %s", err.Error())
	}
}

//бенчмарк добавления объекта
func BenchmarkAddUpdateObject(b *testing.B) {
	hashmap, err = CreatePersistentMemoryHashMap("C:\\Temp", "test.db", "test")
	if err != nil {
		fmt.Errorf("error creating hashmap")
		return
	}
	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		value := "test value " + key
		hashmap.AddUpdateObject(key, []byte(value))
	}
}
