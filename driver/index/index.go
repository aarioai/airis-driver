package index

import "slices"

type IndexType uint8
type IndexColumn struct {
	Column    string
	Asc       bool // default is DESC
	Invisible bool
	Type      IndexType
	Language  string
}
type Indexes map[string][]IndexColumn
type Entity interface {
	Table() string
	Indexes() Indexes
}

const (
	PrimaryT IndexType = iota
	UniqueT
	IndexT
	TextT // MySQL FullText, Mongodb Text
	SpatialT

	Spatial2DT
	Spatial2DSphereT
	HashedT
)

func index(t IndexType, fields ...string) []IndexColumn {
	if len(fields) == 1 {
		return []IndexColumn{{Type: t, Column: fields[0]}}
	}
	indexes := make([]IndexColumn, 0, len(fields))
	for _, field := range fields {
		indexes = append(indexes, IndexColumn{Type: t, Column: field})
	}
	return indexes
}
func Primary(fields ...string) []IndexColumn {
	return index(PrimaryT, fields...)
}
func Unique(fields ...string) []IndexColumn {
	return index(UniqueT, fields...)
}
func Index(fields ...string) []IndexColumn {
	return index(IndexT, fields...)
}
func FullText(fields ...string) []IndexColumn {
	return index(TextT, fields...)
}
func Spatial(fields ...string) []IndexColumn {
	return index(SpatialT, fields...)
}
func Spatial2D(fields ...string) []IndexColumn {
	return index(Spatial2DT, fields...)
}
func Spatial2DSphere(fields ...string) []IndexColumn {
	return index(Spatial2DSphereT, fields...)
}
func Hashed(fields ...string) []IndexColumn {
	return index(HashedT, fields...)
}

func (s Indexes) Primary() []string {
	for _, columns := range s {
		if len(columns) == 0 {
			continue
		}
		if columns[0].Type == PrimaryT {
			fields := make([]string, len(columns))
			for i, column := range columns {
				fields[i] = column.Column
			}
			return fields
		}
	}
	return nil
}

// List all indexes of these types
// E.g. List(UniqueT, IndexT)
func (s Indexes) List(types ...IndexType) map[string][]string {
	indexes := make(map[string][]string)
	for name, columns := range s {
		if len(columns) == 0 {
			continue
		}
		if slices.Contains(types, columns[0].Type) {
			fields := make([]string, len(columns))
			for i, column := range columns {
				fields[i] = column.Column
			}
			indexes[name] = fields
		}
	}
	if len(indexes) == 0 {
		return nil
	}
	return indexes
}
