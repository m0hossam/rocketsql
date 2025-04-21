package compiler

import "github.com/m0hossam/rocketsql/plan"

func CompileSQL(sql string) (*plan.Plan, error) {
	return plan.NewPlan(), nil
}
