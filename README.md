# TableORM
## 简介
通过ORM的方式简化阿里云TableStore使用。
## 项目起源
一直想找一个合适的serverless数据库，看上了阿里云的TableStore，但是API设计的太繁琐，于是就写了这个项目打算封装一下，不过后来发现用了套路云多元索引后强制预留CU资源，每个小时都会产生费用，因此serverless就没什么意义了，不过代码还是放上来，供不差钱的朋友们使用吧。

## 约定

+ 为了简化逻辑，约定每个结构体都有一个ID(_id)字段，作为TableStore的主键，有且仅有一个，其余字段作为column存在。
+ 每个字段默认自动创建对应类型的多元索引，可以通过tag禁止创建对应字段的索引。

## 使用
```go

import (
	"github.com/diemus/tableorm"
	"github.com/diemus/tableorm/query"
)

type User struct {
	ID       string `json:"_id"` //默认必须有的字段，作为主键
	Username string `json:"username" index:"text"` //索引会根据类型进行推断，但是也可以主动指定
	Age      int64  `json:"age"`
	Extra    string `json:"extra" index:"-"` //表示不建立索引
}

type Book struct {
	ID      string  `json:"_id"`
	Caption string  `json:"caption" index:"-"` //不开启索引
	Test    string  `json:"-"`                 //不储存到tablestore
	Count   int64   `json:"count"`
	IsReady bool    `json:"isReady"`
	Price   float64 `json:"price"`
}

func main() {
	db := tableorm.NewDB(
		"https://xxxxx.ots.aliyuncs.com",
		"xxxx",
		"xxxx",
		"xxxxx",
	)

	//自动建表+索引
	db.AutoMigrate(User{}, Book{})

	//创建+修改
	user1 := User{Username: "sam", Age: 16}
	user2 := User{Username: "tom", Age: 32}
	db.Save(user1, user2) //可以传入多个，批量创建

	//查询
	user := User{}
	db.Query(query.TermQuery("username", "sam")).Find(&user)

	//复杂查询
	q1 := query.Not(query.TermQuery("username", "tom"))
	q2 := query.And(query.TermsQuery("age", 10, 12, 13), query.RangeQuery("age", ">", 15))
	q3 := query.Or(q1, q2)
	db.Query(q1,q2,q3).Find(&user)

	//删除
	db.Delete(user1,user2) //可以传入多个，批量删除
}


```