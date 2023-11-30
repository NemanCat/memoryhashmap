//шина событий
//hashmap в памяти с сохранением состояния в БД Bolt
//в качестве ключей map допускаются только строки

package eventbus

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

type PersistentMemoryHashMap struct {
	//map в памяти
	list map[string][]byte
	//директория файла БД Bolt
	dbfolder string
	//имя файла БД Bolt
	dbfile string
	//имя bucket
	bucket string
	//флаг доступности сохранения данных на диске
	//если false то данные на диске не сохраняются, функционирует как MemoryHashMap
	is_persistence_available bool
}

// ------------------------------------------------------
// количество объектов в списке
func (pmhm *PersistentMemoryHashMap) Count() int {
	lock := &sync.RWMutex{}
	lock.RLock()
	defer lock.RUnlock()
	return len(pmhm.list)
}

// получение списка всех элементов map
func (pmhm *PersistentMemoryHashMap) GetList() map[string][]byte {
	lock := &sync.RWMutex{}
	lock.RLock()
	defer lock.RUnlock()
	return pmhm.list
}

// поиск элемента map по ключу
// key ключ искомого элемента
// возвращает nil если в map отсутствует элемент с таким ключом
func (pmhm *PersistentMemoryHashMap) FindByKey(key string) []byte {
	rec, found := pmhm.list[key]
	if !found {
		return nil
	}
	return rec
}

// добавление нового элемента в map
// если элемент с таким ключом уже имеется в map он получит новое значение
// если элемент с таким ключом отсутствует в map будет добавлен новый элемент
// key   ключ элемента
// value значение нового элемента
func (pmhm *PersistentMemoryHashMap) AddUpdateObject(key string, value []byte) {
	lock := &sync.RWMutex{}
	lock.Lock()
	defer lock.Unlock()
	pmhm.list[key] = value
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
			_, err := tx.CreateBucketIfNotExists([]byte(pmhm.bucket))
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
				bucket = tx.Bucket([]byte(pmhm.bucket))
				//сохраняем
				err = bucket.Put([]byte(key), value)
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
	delete(pmhm.list, key)
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
			_, err := tx.CreateBucketIfNotExists([]byte(pmhm.bucket))
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
				bucket = tx.Bucket([]byte(pmhm.bucket))
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
// dbfolder директория в которой располагается файл БД Bolt
// dbfile   имя файла БД Bolt
// bucket   имя bucket
// возвращает ссылку на новый объект типа
// возвращает ошибку доступа к БД Bolt
// в случае наличия ошибки доступа к БД объект может использоваться для хранения данных только в памяти
func CreatePersistentMemoryHashMap(dbfolder string, dbfile string, bucket string) (*PersistentMemoryHashMap, error) {
	pmhm := new(PersistentMemoryHashMap)
	pmhm.list = make(map[string][]byte)
	pmhm.dbfolder = dbfolder
	pmhm.dbfile = dbfile
	pmhm.bucket = bucket
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
		_, err := tx.CreateBucketIfNotExists([]byte(pmhm.bucket))
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
		bucket := tx.Bucket([]byte(pmhm.bucket))
		if bucket == nil {
			return errors.New("bucket does not exist")
		}
		bucket.ForEach(func(k, v []byte) error {
			pmhm.list[string(k)] = v
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
