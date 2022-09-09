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
