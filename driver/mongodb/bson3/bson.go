package bson3

import (
	"github.com/aarioai/airis-driver/driver/mongodb/bson2"
	"github.com/aarioai/airis/pkg/types"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var (
	compareOperators = map[string]string{
		"=":  "$eq",
		">":  "$gt",
		">=": "$gte",
		"<":  "$lt",
		"<=": "$lte",
		"!=": "$ne",
	}

	bitOperators = map[string]string{
		"&": "and",
		"|": "or",
		"^": "xor",
	}
)

// C compare
// E.g. C("a", ">=", 100), C("a", "$gte", 100)
// operator:
// = > >= < <= != $eq $gt $gte $lt $lte $ne
// & | ^
// $type
// $in $nin $all $size $elemMatch $addToSet $pop $pull $push $pushAll $each $position $slice $sort
func C(field, operator string, value any) bson.M {
	// = > >= < <= != $eq $gt $gte $lt $lte $ne
	if v, ok := compareOperators[operator]; ok {
		return bson.M{field: bson.M{operator: v}}
	}
	for k, v := range bitOperators {
		if k == operator || v == operator {
			return Bit(field, v, value.(int))
		}
	}

	return bson.M{field: bson.M{operator: value}}
}

func In(field string, values ...any) bson.M {
	return bson.M{field: bson2.In(values...)}
}

// Nin field not in
func Nin(field string, values ...any) bson.M {
	return bson.M{field: bson2.Nin(values...)}
}

func NotMatch(field string, pattern string, opts ...string) bson.M {
	return bson.M{field: bson2.NotMatch(pattern, opts...)}
}

func Exists(field string) bson.M {
	return bson.M{field: bson2.Exists()}
}

func NotExists(field string) bson.M {
	return bson.M{field: bson2.NotExists()}
}

func Type(field string, t bson.Type) bson.M {
	return bson.M{field: bson2.Type(t)}
}

func Types(field string, ts ...bson.Type) bson.M {
	return bson.M{field: bson2.Types(ts...)}
}

func Mod[T, T2 types.Number](field string, divisor T, remainder T2) bson.M {
	return bson.M{field: bson2.Mod(divisor, remainder)}
}

// All search array filed contains all values elements
// E.g. All("scores", 10, 20, 30)  ==>  scores.contains([10,20,30]), [1,10,20,21,30] True, [20,30] False
func All(field string, values ...any) bson.M {
	return bson.M{field: bson2.All(values...)}
}

// ElemMatch
// E.g. ElemMatch("scores", {"$gt":10, "$lt":50})		scores = [10,20,30,40]
// ElemMatch("results", {product:"xyz", score:{$gte:8}})    results = [{product, score},{product, score}]
func ElemMatch(field string, match bson.M) bson.M {
	return bson.M{field: bson2.ElemMatch(match)}
}

// Size
// E.g. Size("scores", 2)  ==> matches scores contains and only contains 2 elements
func Size(field string, size int) bson.M {
	return bson.M{field: bson2.Size(size)}
}

// Bit
// E.g. Bit("type", "&", 10), Bit("type", "^", 2)
func Bit(field string, operator string, value int) bson.M {
	if s, ok := bitOperators[operator]; ok {
		operator = s
	}
	return bson.M{"$bit": bson.M{
		field: bson.M{operator: value},
	}}
}

// BitsAllClear
// E.g. BitsAllClear("command", 0, 2)  ==>  b11111010 TRUE, b0000010 TRUE, b100 FALSE
func BitsAllClear(field string, bitmasks ...int) bson.M {
	return bson.M{field: bson2.BitsAllClear(bitmasks...)}
}

func BitsAllSet(field string, bitmasks ...int) bson.M {
	return bson.M{field: bson2.BitsAllSet(bitmasks...)}
}

func BitsAnyClear(field string, bitmasks ...int) bson.M {
	return bson.M{field: bson2.BitsAnyClear(bitmasks...)}
}

func BitsAnySet(field string, bitmasks ...int) bson.M {
	return bson.M{field: bson2.BitsAnySet(bitmasks...)}
}

func Slice(field string, n int) bson.M {
	return bson.M{field: bson2.Slice(n)}
}

func SliceSkip(field string, skip, n int) bson.M {
	return bson.M{field: bson2.SliceSkip(skip, n)}
}

func Pop(field string, n int) bson.M {
	return bson2.Pop(bson.M{field: n})
}

func Pull(field string, n int) bson.M {
	return bson2.Pull(bson.M{field: n})
}

func Push(field string, value interface{}) bson.M {
	return bson2.Push(bson.M{field: value})
}

func PushAll(field string, values bson.A) bson.M {
	return bson2.PushAll(bson.M{field: values})
}

func Each(field string, values bson.A) bson.M {
	return bson.M{field: bson2.Each(values)}
}

func PushTo(field string, values bson.A, position int) bson.M {
	return bson.M{"$push": bson.M{
		field: bson.M{
			"$each":     values,
			"$position": position,
		},
	}}
}

func PushAndSlice(field string, values bson.A, n int) bson.M {
	return bson.M{"$push": bson.M{
		field: bson.M{
			"$each":  values,
			"$slice": n,
		},
	}}
}

func PushAndSort(field string, values bson.A, sort any) bson.M {
	return bson.M{"$push": bson.M{
		field: bson.M{
			"$each": values,
			"$sort": sort,
		},
	}}
}

func PushAndSortAsc(field string, values bson.A) bson.M {
	return PushAndSort(field, values, 1)
}

func PushAndSortAscBy(field string, values bson.A, key string) bson.M {
	return PushAndSort(field, values, bson.M{key: 1})
}

func PushAndSortDesc(field string, values bson.A) bson.M {
	return PushAndSort(field, values, -1)
}

func PushAndSortDescBy(field string, values bson.A, key string) bson.M {
	return PushAndSort(field, values, bson.M{key: -1})
}
