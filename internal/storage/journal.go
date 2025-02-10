package storage

const ( // journal constants
	offsetofNumPages          = 0
	offsetofPageSize          = 4
	offsetofDbInitialNumPages = 8
	offsetofJournalPages      = 12
	sizeofJournalConst        = 4 // includes the 3 consts in the header and the page no. preceding each journal page
)

type journal struct {
	filePath       string
	nPages         uint32
	pageSize       uint32
	dbInitNumPages uint32
	pages          []*page
}

func createJournal(journalFilePath string, dbPageSize uint32, dbNumPages uint32) *journal {
	err := createFile(journalFilePath)
	if err != nil {
		return nil
	}

	return &journal{
		filePath:       journalFilePath,
		nPages:         0,
		pageSize:       dbPageSize,
		dbInitNumPages: dbNumPages,
		pages:          make([]*page, 0),
	}
}

func loadJournal(journalFilePath string) *journal {
	b, err := loadJournalFromDisk(journalFilePath)
	if err != nil {
		return nil
	}

	j := deserializeJournal(b)
	j.filePath = journalFilePath
	return j
}

func (j *journal) appendPage(p *page) {
	j.nPages++
	j.pages = append(j.pages, p)
}

func (j *journal) flush() error {
	b := serializeJournal(j)
	return flushFile(j.filePath, b)
}

func (j *journal) delete() error {
	return deleteFile(j.filePath)
}
