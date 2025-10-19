package main

import (
	"fmt"
	"strconv"
	"syscall/js"

	"github.com/m0hossam/rocketsql/db"
	"github.com/m0hossam/rocketsql/page"
	"github.com/m0hossam/rocketsql/pager"
	"github.com/m0hossam/rocketsql/record"
)

var rocketsql *db.Db

func executeSQL(this js.Value, args []js.Value) interface{} {
	if len(args) == 0 {
		return "no SQL provided"
	}

	sql := args[0].String()

	// Meta-commands
	if len(sql) >= 1 {
		if sql[0] == '.' {
			return rocketsql.ExecuteMetaCommand(sql)
		}
	}

	// Execute SQL
	rowsAffected, resultTable, err := rocketsql.ExecuteSQL(sql)
	if err != nil {
		return fmt.Sprint(err)
	}

	// Print DML, DDL results
	if resultTable == nil {
		return fmt.Sprintf("%d row(s) affected", rowsAffected)
	}

	// Print query results
	output := ""
	if err = resultTable.BeforeFirst(); err != nil {
		return fmt.Sprint(err)
	}

	for {
		next, err := resultTable.Next()

		if err != nil {
			return fmt.Sprint(err)
		}

		if !next {
			break
		}

		output += resultTable.GetRow() + "\n"
	}

	return output
}

func pageToJSON(pg *page.Page) js.Value {
	obj := js.Global().Get("Object").New()
	obj.Set("Id", pg.Id)
	if pg.Type == page.LeafPage {
		obj.Set("Type", "Leaf Page")
	} else {
		obj.Set("Type", "Interior Page")
	}
	obj.Set("CellArrOff", pg.CellArrOff)
	obj.Set("NumFragBytes", pg.NumFragBytes)
	obj.Set("LastPtr", pg.LastPtr)
	obj.Set("NumCells", pg.NumCells)

	freeBlocks := js.Global().Get("Array").New()
	for pg.FreeList != nil {
		freeBlock := js.Global().Get("Object").New()
		freeBlock.Set("Offset", pg.FreeList.Offset)
		freeBlock.Set("Size", pg.FreeList.Size)
		if pg.FreeList.Next != nil {
			freeBlock.Set("NextOff", pg.FreeList.Next.Offset)
		} else {
			freeBlock.Set("NextOff", page.DbNullPage)
		}
		freeBlocks.Call("push", freeBlock)
		pg.FreeList = pg.FreeList.Next
	}
	obj.Set("FreeBlocks", freeBlocks)

	cellOffsets := js.Global().Get("Array").New()
	for _, off := range pg.CellPtrArr {
		cellOffsets.Call("push", off)
	}
	obj.Set("CellOffsets", cellOffsets)

	cells := js.Global().Get("Array").New()
	for off, c := range pg.Cells {
		cell := js.Global().Get("Object").New()
		cell.Set("Offset", off)
		keyRec, _ := record.NewRecord(c.Key)
		cell.Set("Key", keyRec.ToString())
		if pg.Type == page.LeafPage {
			valRec, _ := record.NewRecord(c.Value)
			cell.Set("Row", valRec.ToString())
		} else {
			cell.Set("Ptr", page.BytesToUint32(c.Value))
		}
		cells.Call("push", cell)
	}
	obj.Set("Cells", cells)

	return obj
}

func getPage(this js.Value, args []js.Value) interface{} {
	if len(args) == 0 {
		return "no number provided"
	}

	var pgNo uint32

	if args[0].Type() == js.TypeNumber {
		pgNo = uint32(args[0].Int())
	} else {
		i64, err := strconv.ParseInt(args[0].String(), 10, 64)
		if err != nil {
			return fmt.Sprint(err)
		}
		pgNo = uint32(i64)
	}

	pg, err := rocketsql.GetPager().ReadPage(pgNo)
	if err != nil {
		return fmt.Sprint(err)
	}

	return pageToJSON(pg)
}

func getAllPages(this js.Value, args []js.Value) interface{} {
	pgs := js.Global().Get("Array").New()
	for i := uint32(1); i <= rocketsql.GetPager().GetDbHeader().NumPages; i++ {
		pg, err := rocketsql.GetPager().ReadPage(i)
		if err != nil {
			return fmt.Sprint(err)
		}
		pgs.Call("push", pageToJSON(pg))
	}
	return pgs
}

func jsNodeFromGo(n *pager.Node) js.Value {
	jsNode := js.Global().Get("Object").New()
	jsNode.Set("Id", n.Id)
	if n.Type == page.InteriorPage {
		jsNode.Set("Type", "Interior Node")
	} else {
		jsNode.Set("Type", "Leaf Node")
	}

	if n.Type == page.InteriorPage {
		childrenArr := js.Global().Get("Array").New()
		for _, child := range n.Children {
			childrenArr.Call("push", jsNodeFromGo(child))
		}
		jsNode.Set("Children", childrenArr)
	}

	return jsNode
}

func getAllTables(this js.Value, args []js.Value) interface{} {
	tbls := js.Global().Get("Array").New()
	arr, err := rocketsql.GetAllTables()
	if err != nil {
		return fmt.Sprint(err)
	}
	for _, t := range arr {
		tbl := js.Global().Get("Object").New()
		tbl.Set("Name", t.Name)
		if t.Root != nil {
			tbl.Set("Root", jsNodeFromGo(t.Root))
		}
		tbls.Call("push", tbl)
	}
	return tbls
}

func main() {
	fmt.Println("rocketSQL> Welcome to RocketSQL")
	fmt.Println("rocketSQL> You are connected to an in-memory database")

	var err error
	rocketsql, err = db.NewDb()
	if err != nil {
		fmt.Println(err)
		return
	}

	js.Global().Set("executeSQL", js.FuncOf(executeSQL))
	js.Global().Set("getPage", js.FuncOf(getPage))
	js.Global().Set("getAllPages", js.FuncOf(getAllPages))
	js.Global().Set("getAllTables", js.FuncOf(getAllTables))

	// Prevent exit
	c := make(chan struct{})
	<-c
}
