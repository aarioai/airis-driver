# Mongodb 说明文档

* bson.E    {Key, Value}
* bson.D    有序 []bson.E
* bson.M    无序 map[string]any  => map[string]bson.D or map[string]bson.M
* bson.A    []any =>  bson.A{"apple", 18, bson.M{}, bson.E{}, bson.D{}}

## Session
* startSession
* endSession
* refreshSession
* killSessions
* killAllSessions
* killAllSessionsByPattern

## Query
* 比较查询 **COMPARE**
  * \$eq \$gt \$gte \$lt \$lte \$ne        `bson.M{ <key>: bson.M{"$eq":<value>} }`  等价于 `bson.M{<key>:<value>}`
  * \$in 字段是否在指定数组中                `bson.M{<key>: bson.M{"$exists": true, "$nin": bson.A{<value>, <value>}}}`
  * $nin not in
* 逻辑查询 **LOGICAL**
  * $and $or $nor                        `bson.M{"$nor": bson.A{bson.M{<key>: <value>}...}}`
  * $not                                 `bson.M{"$not": {COMPARE}}`
* 元素查询 **ELEMENT**
  * $exists         某个字段是否存在 `bson.M{<key>: bson.M{"$exists": true, "$nin": bson.A{<value>, <value>}}}`
  * $type double|string|object|array|binData|undefined|objectId|bool|date|null|regex|javascript|int|timestamp|long
    |decimal|minKey|maxKey
* 评估查询
  * $expr           允许在查询语言中使用聚合表达式
  * $jsonSchema     根据给定的 JSON 模式验证文档
  * $mod            对字段值执行模运算，并选择具有指定结果的文档
  * $regex          选择值匹配指定正则表达式的文档
  * $where          匹配满足 JavaScript 表达式的文档
* 地理空间查询
  * $geoIntersects
  * $geoWithin
  * $near
  * $nearSphere
  * $box
  * $center
  * $centerSphere
  * $geometry
  * $maxDistance
  * $minDistance
  * $polygon
* 数组查询
  * $all           匹配包含查询中指定的所有元素的数组  `All("scores", 10, 20, 30)`  ==>  `scores.contains([10,20,30])`, [1,10,20,21,30] True, [20,30] False
  * $elemMatch     如果数组字段中的元素与所有指定的 $elemMatch 条件均匹配，则选择文档
  * $size          如果数组字段达到指定大小，则选择文档 `{"scores":{$size, 2}`  ==> 匹配scores有且只有2个元素
* 按位查询
  * $bitsAllClear  匹配数字或二进制值，其中一组片段位置均包含值0  `{"scores":{$bitsAllClear:[0,2]}}` 匹配（从后往前）第0和第2位为0
  * $bitsAllSet    匹配数字或二进制值，其中一组片段位置均包含值1
  * $bitsAnyClear  匹配数字或二进制值，其中一组位位置中的任何 位的值为 0
  * $bitsAnySet    匹配数字或二进制值，其中一组位位置中的任何 位的值为 1
* 投射
  * $              对数组中与查询条件匹配的第一个元素进行投影
  * $elemMatch     对数组中与指定 $elemMatch 条件匹配的第一个元素进行投影
  * $meta          投影每个文档的可用元数据
  * $slice         限制从数组中投影的元素数量。支持跳过切片和对切片进行数量限制
* 其他
  * $natural       可通过 sort() 或 hint() 方法提供的特殊提示，可用于强制执行正向或反向集合扫描
  * $rand          生成介于 0 和 1 之间的随机浮点数
  
## Update
* 更新字段
  * $currentDate 
  * $inc          为数字型字段增加或减少值
  * $min          若新值小于原始值，则更新，否则不更新
  * $max          若新值大于原始值，则更新，否则不更新
  * $mul          为数字型字段乘以某个值
  * $rename       重命名字段名(key name)，一般当字段名写错了，用此方法修改
  * $set          修改字段值为新值
  * $setOnInsert  若是upsert引起的插入新值，那么会将插入的新值该字段改为这些
  * $unset        删除字段
* 更新数组
  * $             占位符，用于更新与查询条件匹配的第一个元素
  * $[] 	      充当占位符，以更新数组中与查询条件匹配的文档中的所有元素
  * $[<id>]       充当占位符，以更新与查询条件匹配的文档中所有符合 arrayFilters 条件的元素
  * $addToSet     仅向数组中添加尚不存在于该数组的元素
  * $pop          删除数组的第一项或最后一项
  * $pull         删除与指定查询匹配的所有数组元素
  * $push         向数组添加一项
  * $pullAll      从数组中删除所有匹配值
  * $each         改 $push 和 $addToSet 运算符，以在数组更新时追加多个项目
  * $position     修改 $push 运算符，以指定在数组中添加元素的位置
  * $slice        修改 $push 运算符以限制更新后数组的大小
  * $sort         修改 $push 运算符，以对存储在数组中的文档重新排序
* 更新位
  * $bit          and|or|xor