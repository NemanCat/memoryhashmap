// тип MemoryHashMap реализует простой thread-safe map в памяти
// ключи map имеют тип string
// для сохранения в map экземпляров объектов объекты должны реализовывать интерфейс BaseObjectInterface

package memoryhashmap

import (
	"sync"
)

// -----------------------------------------
// хэшированный map в памяти
// может хранить экземпляры любых объектов реализующих интерфейс BaseObjectInterface
type MemoryHashMap struct {
	//список объектов
	list map[string]BaseObjectInterface
}

//-------------------------------------

// количество элементов в map
func (mhm *MemoryHashMap) Count() int {
	lock := &sync.RWMutex{}
	lock.RLock()
	defer lock.RUnlock()
	return len(mhm.list)
}

// получение списка всех элементов map
func (mhm *MemoryHashMap) GetList() map[string]BaseObjectInterface {
	lock := &sync.RWMutex{}
	lock.RLock()
	defer lock.RUnlock()
	return mhm.list
}

// поиск элемента map по ключу
// key ключ искомого элемента
// возвращает nil если в map отсутствует элемент с таким ключом
func (mhm *MemoryHashMap) FindByKey(key string) BaseObjectInterface {
	lock := &sync.RWMutex{}
	lock.RLock()
	defer lock.RUnlock()
	value, ok := mhm.list[key]
	if !ok {
		value = nil
	}
	return value
}

// добавление нового элемента/редактирование существующего элемента в map
// если элемент с таким ключом уже имеется в map он получит новое значение
// если элемент с таким ключом отсутствует в map будет добавлен новый элемент
// key   ключ элемента
// value значение нового элемента
func (mhm *MemoryHashMap) AddUpdateObject(key string, value BaseObjectInterface) {
	lock := &sync.RWMutex{}
	lock.Lock()
	defer lock.Unlock()
	mhm.list[key] = value
}

// удаление из списка элемента с указанным ключом
// key ключ удаляемого элемента
func (mhm *MemoryHashMap) DeleteObject(key string) {
	lock := &sync.RWMutex{}
	lock.Lock()
	defer lock.Unlock()
	delete(mhm.list, key)
}

// -----------------------------------------------------
// создание map
func CreateMemoryHashMap() *MemoryHashMap {
	return &MemoryHashMap{list: make(map[string]BaseObjectInterface)}
}
