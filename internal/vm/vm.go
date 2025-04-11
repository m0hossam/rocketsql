package vm

import "github.com/m0hossam/rocketsql/internal/storage"

type Vm struct {
	Btree *storage.Btree
}

func NewVm(btree *storage.Btree) *Vm {
	return &Vm{
		Btree: btree,
	}
}
