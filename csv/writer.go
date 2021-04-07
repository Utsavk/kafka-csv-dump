package csv

type Writer struct {
	FilePrefix   string
	MaxRows      int
	ChunkRows    int
	FileBasePath string
	Compress     bool
	Data         [][]string
	Filenames    []string
}

func NewWriter() *Writer {
	return nil
}

func (w *Writer) CloseWriter() []string {
	w.forceWrite()
	w.Data = nil
	fileNames := w.Filenames
	w = nil
	return fileNames
}

func (w *Writer) Write() error {
	return nil
}

func (w *Writer) forceWrite() error {
	return nil
}
