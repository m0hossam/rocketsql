package planner

import (
	"github.com/m0hossam/rocketsql/metadata"
	"github.com/m0hossam/rocketsql/processor"
)

type Plan interface {
	Open() (*processor.Scan, error)
	MetaData() (*metadata.TableMetadata, error)
}

type TablePlan struct {
}
