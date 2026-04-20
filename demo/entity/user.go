package entity

import (
	"github.com/aarioai/airis-driver/driver/index"
	"github.com/aarioai/airis/aa/atype"
	"github.com/aarioai/airis/aa/atype/aenum"
)

type User struct {
	Uid      uint64           `db:"uid" comment:"用户ID"`
	Username atype.NullString `db:"username" comment:"用户名"`
	PhoneNum string           `db:"phone_num" comment:"phone_num"`

	Status    aenum.Status   `db:"status"`
	CreatedAt atype.Datetime `db:"created_at"`
	UpdatedAt atype.Datetime `db:"updated_at"`
}

func (t User) Table() string {
	return "user"
}

func (t User) Indexes() index.Indexes {
	return index.NewIndexes(
		index.Primary("id"),
		index.Unique("username"),
		index.Unique("phone_num"),
	)
}
