package bson2

import (
	"github.com/aarioai/airis/pkg/afmt"
	"github.com/aarioai/airis/pkg/types"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func In(values ...any) bson.M {
	return bson.M{"$in": A(values...)}
}

// Nin not in
func Nin(values ...any) bson.M {
	return bson.M{"$nin": A(values...)}
}

func NotMatch(pattern string, opts ...string) bson.M {
	re := bson.Regex{Pattern: pattern, Options: afmt.First(opts)}
	return bson.M{"$not": re}
}

// And
// E.g. And(In("a", 10, 20, 30), bson.M{"status": 0}, C("x", "!=", 300))
func And(values ...any) bson.M {
	return bson.M{"$and": A(values...)}
}

func Or(values ...any) bson.M {
	return bson.M{"$or": A(values...)}
}

// Nor not or, not match any of these
func Nor(values ...any) bson.M {
	return bson.M{"$nor": A(values...)}
}

func Exists() bson.M {
	return bson.M{"$exists": true}
}

func NotExists() bson.M {
	return bson.M{"$exists": false}
}

func Type(t bson.Type) bson.M {
	return bson.M{"$type": t}
}

func Types(types ...bson.Type) bson.M {
	ts := make([]any, 0, len(types))
	for _, t := range types {
		ts = append(ts, t)
	}
	return bson.M{"$type": bson.A(ts)}
}

func Mod[T, T2 types.Number](divisor T, remainder T2) bson.M {
	return bson.M{"$mod": bson.A{divisor, remainder}}
}

func All(values ...any) bson.M {
	return bson.M{"$all": A(values...)}
}

func ElemMatch(match bson.M) bson.M {
	return bson.M{"$elemMatch": match}
}

func Size(size int) bson.M {
	return bson.M{"$size": size}
}

func BitsAllClear(bitmasks ...int) bson.M {
	ts := make([]any, 0, len(bitmasks))
	for _, t := range bitmasks {
		ts = append(ts, t)
	}
	return bson.M{"$bitsAllClear": ts}
}

func BitsAllSet(bitmasks ...int) bson.M {
	ts := make([]any, 0, len(bitmasks))
	for _, t := range bitmasks {
		ts = append(ts, t)
	}
	return bson.M{"$bitsAllSet": ts}
}

func BitsAnyClear(bitmasks ...int) bson.M {
	ts := make([]any, 0, len(bitmasks))
	for _, t := range bitmasks {
		ts = append(ts, t)
	}
	return bson.M{"$bitsAnyClear": ts}
}

func BitsAnySet(bitmasks ...int) bson.M {
	ts := make([]any, 0, len(bitmasks))
	for _, t := range bitmasks {
		ts = append(ts, t)
	}
	return bson.M{"$bitsAnySet": ts}
}

func Slice(n int) bson.M {
	return bson.M{"$slice": n}
}

func SliceSkip(skip, n int) bson.M {
	return bson.M{"$slice": bson.A{skip, n}}
}

func Pop(m bson.M) bson.M {
	return bson.M{"$pop": m}
}

func Pull(m bson.M) bson.M {
	return bson.M{"$pull": m}
}

func Push(m bson.M) bson.M {
	return bson.M{"$push": m}
}

func PushAll(m bson.M) bson.M {
	return bson.M{"$pushAll": m}
}

func Each(values bson.A) bson.M {
	return bson.M{"$each": values}
}
