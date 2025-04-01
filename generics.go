package database

func DecodeList[T any](l List) (map[string]T, error) {
	out := make(map[string]T)
	for id, item := range l {
		var gen T
		if err := item.Decode(&gen); err != nil {
			return out, err
		}
		out[id] = gen
	}
	return out, nil
}

func DecodeListToSlice[T any](l List) ([]T, error) {
	var out []T
	for _, item := range l {
		var gen T
		if err := item.Decode(&gen); err != nil {
			return out, err
		}
	}
	return out, nil
}

func GetAll[T any](db Database) (map[string]T, error) {
	output := make(map[string]T)
	f := func(id string, item Decoder) error {
		var gen T
		if err := item.Decode(&gen); err != nil {
			return err
		}
		output[id] = gen
		return nil
	}
	if err := db.ForEach(f); err != nil {
		return output, err
	}
	return output, nil
}

func GetAllSlice[T any](db Database) ([]T, error) {
	var output []T
	f := func(id string, item Decoder) error {
		var gen T
		if err := item.Decode(&gen); err != nil {
			return err
		}
		output = append(output, gen)
		return nil
	}
	if err := db.ForEach(f); err != nil {
		return output, err
	}
	return output, nil
}

type GenericNode[T any] struct {
	db Database
}

func NewGenericNode[T any](db Database) (*GenericNode[T], error) {
	gn := &GenericNode[T]{db}
	return gn, nil
}

func (gn *GenericNode[T]) Get(id string) (T, error) {
	var item T

	if err := gn.db.Get(id, &item); err != nil {
		return item, err
	}
	return item, nil
}

func (gn *GenericNode[T]) GetAll() (map[string]T, error) {
	return GetAll[T](gn.db)
}

func (gn *GenericNode[T]) ForEach(gf func(string, T) error) error {
	f := func(id string, item Decoder) error {
		var gen T
		if err := item.Decode(&gen); err != nil {
			return err
		}
		return gf(id, gen)
	}
	return gn.db.ForEach(f)
}

func (gn *GenericNode[T]) Set(id string, item T) error {
	return gn.db.Set(id, item)
}

/*
func (gn *GenericNode[T]) Pages(count int) int {
	return gn.db.Pages(count)
}

func (gn *GenericNode[T]) Range(page, count int) (map[string]T, error) {
	out := make(map[string]T)

	return out, ErrorNotImplemented
}
*/
