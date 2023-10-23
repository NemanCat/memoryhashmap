package memoryhashmap

// базовый интерфейс для объектов хранящихся в map
type BaseObjectInterface interface {
	//для всех типов элементов должен быть реализован метол декодирования объекта из JSON
	UnmarshalBaseObjectJSON([]byte) string
}
