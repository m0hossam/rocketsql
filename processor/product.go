package processor

type ProductScan struct {
	s1 Scan
	s2 Scan
}

func NewProductScan(s1, s2 Scan) *ProductScan {
	return &ProductScan{
		s1: s1,
		s2: s2,
	}
}

func (ps *ProductScan) BeforeFirst() error {
	if err := ps.s1.BeforeFirst(); err != nil {
		return err
	}

	if _, err := ps.s1.Next(); err != nil { // Must do this because we advance s2 in the Next()
		return err
	}

	return ps.s2.BeforeFirst()
}

func (ps *ProductScan) Next() (bool, error) {
	next, err := ps.s2.Next() // Advance s2

	if err != nil {
		return false, err
	}

	if !next { // If we looped through s2
		next, err = ps.s1.Next() // Advance s1
		if !next || err != nil { // If we looped through s1 (and consequently s2) or encountered an error
			return false, err
		}

		if err = ps.s2.BeforeFirst(); err != nil { // Reset s2
			return false, err
		}

		return ps.s2.Next() // Adavance s2
	}

	return true, nil
}

func (ps *ProductScan) GetInt16(colName string) (int16, error) {
	if val, err := ps.s1.GetInt16(colName); err == nil {
		return val, nil
	}

	return ps.s2.GetInt16(colName)
}

func (ps *ProductScan) GetInt32(colName string) (int32, error) {
	if val, err := ps.s1.GetInt32(colName); err == nil {
		return val, nil
	}

	return ps.s2.GetInt32(colName)
}

func (ps *ProductScan) GetInt64(colName string) (int64, error) {
	if val, err := ps.s1.GetInt64(colName); err == nil {
		return val, nil
	}

	return ps.s2.GetInt64(colName)
}

func (ps *ProductScan) GetFloat32(colName string) (float32, error) {
	if val, err := ps.s1.GetFloat32(colName); err == nil {
		return val, nil
	}

	return ps.s2.GetFloat32(colName)
}

func (ps *ProductScan) GetFloat64(colName string) (float64, error) {
	if val, err := ps.s1.GetFloat64(colName); err == nil {
		return val, nil
	}

	return ps.s2.GetFloat64(colName)
}

func (ps *ProductScan) GetString(colName string) (string, error) {
	if val, err := ps.s1.GetString(colName); err == nil {
		return val, nil
	}

	return ps.s2.GetString(colName)
}

func (ps *ProductScan) GetType(colName string) (string, error) {
	if val, err := ps.s1.GetType(colName); err == nil {
		return val, nil
	}

	return ps.s2.GetType(colName)
}

func (ps *ProductScan) HasColumn(colName string) bool {
	return ps.s1.HasColumn(colName) || ps.s2.HasColumn(colName)
}
