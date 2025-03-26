package bson2

import (
	"go.mongodb.org/mongo-driver/v2/bson"
)

func E(k string, v any) bson.E {
	return bson.E{Key: k, Value: v}
}

func D(kvs ...any) bson.D {
	d := make(bson.D, 0, len(kvs)/2)
	for i := 0; i < len(kvs); i += 2 {
		d = append(d, E(kvs[i].(string), kvs[i+1]))
	}
	return d
}

func A(values ...any) bson.A {
	a := make(bson.A, 0, len(values))
	for _, v := range values {
		a = append(a, v)
	}
	return a
}

func Rand() bson.M {
	return bson.M{"$rand": bson.M{}}
}

// Inc
// E.g. Inc({"score":-2, "max_score":1})
func Inc(m bson.M) bson.M {
	return bson.M{"$inc": m}
}

// Min insert/update matches value less than these in m
func Min(m bson.M) bson.M {
	return bson.M{"$min": m}
}

// Max insert/update
func Max(m bson.M) bson.M {
	return bson.M{"$max": m}
}

func Mul(m bson.M) bson.M {
	return bson.M{"$mul": m}
}

func Rename(m bson.M) bson.M {
	return bson.M{"$rename": m}
}

func Set(m bson.M) bson.M {
	return bson.M{"$set": m}
}

func SetOrInsert(m bson.M) bson.M {
	return bson.M{"$setOrInsert": m}
}

func Unset(keys ...string) bson.M {
	ts := make([]bson.M, 0, len(keys))
	for _, key := range keys {
		ts = append(ts, bson.M{key: ""})
	}
	return bson.M{"$unset": ts}
}

func AddToSet(m bson.M) bson.M {
	return bson.M{"$addToSet": m}
}
