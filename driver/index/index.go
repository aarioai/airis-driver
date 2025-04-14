package index

import (
	"slices"
	"strings"
)

type IndexType uint8
type IndexColumn struct {
	Field     string
	Asc       bool // default is DESC
	Invisible bool
	Type      IndexType
	Language  string
}
type Indexes map[string][]IndexColumn
type Entity interface {
	Table(...any) string
	Indexes() Indexes
}

const (
	PrimaryIndexName = "PRIMARY"
)

const (
	PrimaryT IndexType = iota
	UniqueT
	IndexT
	FullTextT // MySQL fulltext, Mongodb text
	SpatialT

	Spatial2DT
	Spatial2DSphereT
	HashedT
)

func index(t IndexType, fields ...string) []IndexColumn {
	if len(fields) == 1 {
		return []IndexColumn{{Type: t, Field: fields[0]}}
	}
	indexes := make([]IndexColumn, 0, len(fields))
	for _, field := range fields {
		indexes = append(indexes, IndexColumn{Type: t, Field: field})
	}
	return indexes
}
func parseFieldNames(columns []IndexColumn) []string {
	fields := make([]string, len(columns))
	for i, column := range columns {
		fields[i] = column.Field
	}
	return fields
}
func makeIndexName(t IndexType, columns []IndexColumn) string {
	var s strings.Builder
	switch t {
	case PrimaryT:
		return PrimaryIndexName
	case UniqueT:
		s.WriteString("u")
	case IndexT:
		s.WriteString("i")
	case FullTextT:
		s.WriteString("t")
	case SpatialT:
		s.WriteString("s")
	case Spatial2DT:
		s.WriteString("d")
	case Spatial2DSphereT:
		s.WriteString("p")
	case HashedT:
		s.WriteString("h")
	}
	for _, column := range columns {
		field := strings.ReplaceAll(column.Field, "_", "")
		s.WriteByte('_')
		s.WriteString(field)
	}
	return s.String()
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
	return index(FullTextT, fields...)
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

func NewIndexes(indexes ...[]IndexColumn) Indexes {
	idx := make(Indexes, len(indexes))
	for _, columns := range indexes {
		if len(columns) == 0 {
			continue
		}
		idxName := makeIndexName(columns[0].Type, columns)
		idx[idxName] = columns
	}
	return idx
}

func (s Indexes) Primary(indexName ...string) []string {
	name := PrimaryIndexName
	if len(indexName) > 0 {
		name = indexName[0]
	}
	if columns, ok := s[name]; ok {
		return parseFieldNames(columns)
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
			indexes[name] = parseFieldNames(columns)
		}
	}
	if len(indexes) == 0 {
		return nil
	}
	return indexes
}
