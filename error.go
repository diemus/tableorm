package tableorm

import "fmt"

var (
	NotResultFound  = fmt.Errorf("no result found")
	NotAllSuccess   = fmt.Errorf("no all success")
	IDFieldNotExist = fmt.Errorf(`primary key "_id" must be define`)
)
