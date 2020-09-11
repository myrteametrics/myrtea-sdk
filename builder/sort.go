package builder

import "github.com/olivere/elastic"

// SortByDoc is a default sorting method (by document index)
type SortByDoc struct{}

// Source returns an elastic sorter
func (s SortByDoc) Source() elastic.Sorter {
	return elastic.SortByDoc{}
}

// NewSortByDoc returns a new default SortByDoc
func NewSortByDoc() SortByDoc {
	return SortByDoc{}
}

// FieldSort is a specific sorting method using a sorting field
type FieldSort struct {
	Field string `json:"field"`
	Order string `json:"order" enums:"asc,desc"`
}

// NewFieldSort returns a new FieldSort based a on field and an order (asc, desc)
func NewFieldSort(field string, order string) FieldSort {
	return FieldSort{Field: field, Order: order}
}

// Source returns an elastic sorter
func (s FieldSort) Source() elastic.Sorter {
	sorter := elastic.NewFieldSort(s.Field).Asc()
	if s.Order == "desc" {
		sorter = sorter.Desc()
	}
	return sorter
}
