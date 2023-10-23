// тип PersistentMemoryHashMap реализует простой thread-safe map в памяти с сохранением его элементов на диске
// элементы map сохраняются в key - value БД Bolt
// при создании объекта типа осуществляется попытка загрузки в map данных из БД Bolt
// в случае невозможности по какой-либо причине подключиться к БД Bolt данные хранятся только в памяти
// ключи map имеют тип string
// для сохранения в map экземпляров объектов объекты должны реализовывать интерфейс BaseObjectInterface

package memoryhashmap

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// -----------------------------------------
// хэшированный map в памяти с сохранением элементов на диск
// элементы map сохраняются в key - value хранилище Bolt
// может хранить любые объекты реализующие интерфейс BaseObjectInterface
type PersistentMemoryHashMap struct {
	//тип объектов хранящихся в map
	objtype reflect.Type
	//map в памяти
	memory_map *MemoryHashMap
	//директория файла БД Bolt
	dbfolder string
	//имя файла БД Bolt
	dbfile string
	//флаг доступности сохранения данных на диске
	//если false то данные на диске не сохраняются, функционирует как MemoryHashMap
	is_persistence_available bool
}

// ----------------------------------------------
// методы типа
// количество объектов в списке
func (pmhm *PersistentMemoryHashMap) Count() int {
	lock := &sync.RWMutex{}
	lock.RLock()
	defer lock.RUnlock()
	return len(pmhm.memory_map.list)
}

// получение списка всех элементов map
func (pmhm *PersistentMemoryHashMap) GetList() map[string]BaseObjectInterface {
	lock := &sync.RWMutex{}
	lock.RLock()
	defer lock.RUnlock()
	return pmhm.memory_map.list
}

// поиск элемента map по ключу
// key ключ искомого элемента
// возвращает nil если в map отсутствует элемент с таким ключом
func (pmhm *PersistentMemoryHashMap) FindByKey(key string) BaseObjectInterface {
	return pmhm.memory_map.FindByKey(key)
}

// добавление нового элемента в map
// если элемент с таким ключом уже имеется в map он получит новое значение
// если элемент с таким ключом отсутствует в map будет добавлен новый элемент
// key   ключ элемента
// value значение нового элемента
func (pmhm *PersistentMemoryHashMap) AddUpdateObject(key string, value BaseObjectInterface) {
	lock := &sync.RWMutex{}
	lock.Lock()
	defer lock.Unlock()
	pmhm.memory_map.list[key] = value
	if pmhm.is_persistence_available {
		//пытаемся сохранить новый элемент в БД Bolt
		//проверяем существует ли директория файла БД
		_, err := os.Stat(pmhm.dbfolder)
		if os.IsNotExist(err) {
			//функционал сохранения данных на диск не доступен
			pmhm.is_persistence_available = false
			return
		}
		//открываем БД
		dbpath := filepath.Join(pmhm.dbfolder, pmhm.dbfile)
		db, err := bolt.Open(dbpath, 0600, &bolt.Options{Timeout: 1 * time.Second})
		defer db.Close()
		if err != nil {
			//не удалось открыть БД Bolt, функционал сохранения данных на диск не доступен
			pmhm.is_persistence_available = false
			return
		}
		//создаём в БД bucket с именем default если он не существует
		err = db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte("default"))
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			//не удалось создать bucket, функционал сохранения данных на диск не доступен
			pmhm.is_persistence_available = false
			return
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return
		}
		err = db.Update(
			func(tx *bolt.Tx) error {
				var bucket *bolt.Bucket
				var err error
				bucket = tx.Bucket([]byte("default"))
				//сохраняем
				err = bucket.Put([]byte(key), []byte(encoded))
				if err != nil {
					return err
				}

				return nil
			})
		//закрываем БД
		db.Close()
		if err != nil {
			//функционал сохранения данных на диск не доступен
			pmhm.is_persistence_available = false
		}
	}
}

// удаление из списка элемента с указанным ключом
// key ключ удаляемого элемента
func (pmhm *PersistentMemoryHashMap) DeleteObject(key string) {
	lock := &sync.RWMutex{}
	lock.Lock()
	defer lock.Unlock()
	delete(pmhm.memory_map.list, key)
	if pmhm.is_persistence_available {
		//проверяем существует ли директория файла БД
		_, err := os.Stat(pmhm.dbfolder)
		if os.IsNotExist(err) {
			//функционал сохранения данных на диск не доступен
			pmhm.is_persistence_available = false
			return
		}
		//пытаемся удалить объект из БД Bolt
		dbpath := filepath.Join(pmhm.dbfolder, pmhm.dbfile)
		db, err := bolt.Open(dbpath, 0600, &bolt.Options{Timeout: 1 * time.Second})
		defer db.Close()
		if err != nil {
			//не удалось открыть БД Bolt, функционал сохранения данных на диск не доступен
			pmhm.is_persistence_available = false
			return
		}
		//создаём в БД bucket с именем default если он не существует
		err = db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte("default"))
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			//не удалось создать bucket, функционал сохранения данных на диск не доступен
			pmhm.is_persistence_available = false
			return
		}
		err = db.Update(
			func(tx *bolt.Tx) error {
				var bucket *bolt.Bucket
				var err error
				bucket = tx.Bucket([]byte("default"))
				//сохраняем
				err = bucket.Delete([]byte(key))
				if err != nil {
					return err
				}
				return nil
			})
		if err != nil {
			//функционал сохранения данных на диск не доступен
			pmhm.is_persistence_available = false
		}
	}
}

// ---------------------------------------------
// создание экземпляра объекта
// objtype  тип элементов map
// dbfolder директория в которой располагается файл БД Bolt
// dbfile   имя файла БД Bolt
// возвращает ссылку на новый объект типа
// возвращает ошибку доступа к БД Bolt
// в случае наличия ошибки доступа к БД объект может использоваться для хранения данных только в памяти
func CreatePersistentMemoryHashMap(objtype reflect.Type, dbfolder string, dbfile string) (*PersistentMemoryHashMap, error) {
	pmhm := new(PersistentMemoryHashMap)
	pmhm.objtype = objtype
	pmhm.memory_map = CreateMemoryHashMap()
	pmhm.dbfolder = dbfolder
	pmhm.dbfile = dbfile
	//пытаемся открыть БД Bolt
	dbpath := filepath.Join(dbfolder, dbfile)
	lock := new(sync.RWMutex)
	lock.Lock()
	defer lock.Unlock()

	//проверяем существует ли директория файла БД
	_, err := os.Stat(dbfolder)
	if os.IsNotExist(err) {
		//функционал сохранения данных на диск не доступен
		pmhm.is_persistence_available = false
		return pmhm, err
	}

	db, err := bolt.Open(dbpath, 0600, &bolt.Options{Timeout: 1 * time.Second})

	if err != nil {
		//не удалось открыть БД Bolt, функционал сохранения данных на диск не доступен
		pmhm.is_persistence_available = false
		return pmhm, err
	}
	defer db.Close()

	//создаём в БД bucket с именем default если он не существует
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("default"))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		//не удалось создать bucket, функционал сохранения данных на диск не доступен
		pmhm.is_persistence_available = false
		return pmhm, err
	}
	//пытаемся загрузить данные из БД Bolt
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("default"))
		if bucket == nil {
			return errors.New("bucket does not exist")
		}
		bucket.ForEach(func(k, v []byte) error {
			var tmp = reflect.New(objtype)
			method := tmp.MethodByName("UnmarshalBaseObjectJSON")
			inputs := make([]reflect.Value, 1)
			inputs[0] = reflect.ValueOf(v)
			res := method.Call(inputs)
			message := res[0].String()
			if len(message) != 0 {
				return errors.New(message)
			}
			pmhm.memory_map.AddUpdateObject(string(k), tmp.Interface().(BaseObjectInterface))
			return nil
		})
		return nil
	})

	if err != nil {
		//не удалось загрузить данные из БД Bolt, функционал сохранения данных на диск не доступен
		pmhm.is_persistence_available = false
		return pmhm, err
	}
	pmhm.is_persistence_available = true
	return pmhm, nil
}
