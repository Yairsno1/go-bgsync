package dataaccess

// FmtDef defines the format of a csv data file for a security.
type FmtDef struct {
	delimiter     string
	header        int
	columns       int
	date          string
	time          string
	decimalDigits int
}

// Delimiter gets the delimiter of the csv file.
func (fd FmtDef) Delimiter() string {
	return fd.delimiter
}

// Hdr gets the number of header rows in the csv file.
func (fd FmtDef) Hdr() int {
	return fd.header
}

// Columns gets the number of columns in the csv file.
func (fd FmtDef) Columns() int {
	return fd.columns
}

// Date gets the date format in the csv file's date column.
func (fd FmtDef) Date() string {
	return fd.date
}

// Time gets the time format in the csv file's time column, if time column is not present, empty string is returned.
func (fd FmtDef) Time() string {
	return fd.time
}

// DecimalDigits gets the number of decimal digits in the csv file's price fields.
func (fd FmtDef) DecimalDigits() int {
	return fd.decimalDigits
}

// TargetDef defines the targets; source and destination of a csv data file for a security.
// If the file is source file the Dest field contains the ID of the file to be synched.
type TargetDef struct {
	isSrc bool
	dest  string
}

// IsSrc gets if the csv file is source data file.
func (td TargetDef) IsSrc() bool {
	return td.isSrc
}

// Dest gets the definition ID of the csv file to sync if this is sourcs data file definition; othewise, empty string is returned.
func (td TargetDef) Dest() string {
	return td.dest
}

// FileDef defines a csv data file for a security.
type FileDef struct {
	id        string
	kind      string
	timeFrame string
	symbol    string
	name      string
	desc      string
	path      string
	target    TargetDef
	format    FmtDef
}

// ID gets the definition ID of the csv file.
func (fd FileDef) ID() string {
	return fd.id
}

// Kind gets the security kind, index or currency.
func (fd FileDef) Kind() string {
	return fd.kind
}

// TimeFrame gets the data time frame; daily, 5/30/120 minutes and so on.
func (fd FileDef) TimeFrame() string {
	return fd.timeFrame
}

// Symbol gets the security symbol.
func (fd FileDef) Symbol() string {
	return fd.symbol
}

// Name gets the security name.
func (fd FileDef) Name() string {
	return fd.name
}

// Desc gets the descrition about the security.
func (fd FileDef) Desc() string {
	return fd.desc
}

// Path gets the path to the security csv data file.
func (fd FileDef) Path() string {
	return fd.path
}

// Target gets the target definition of the csv data file.
func (fd FileDef) Target() TargetDef {
	return fd.target
}

// Fmt gets the csv data file format definition.
func (fd FileDef) Fmt() FmtDef {
	return fd.format
}

// FileDefs holds a map of file definitions.
type FileDefs struct {
	defs map[string]FileDef
}

// Contains gets if an item with the specified ID is contained in the map.
func (fds FileDefs) Contains(defID string) bool {
	_, ok := fds.defs[defID]

	return ok
}

// Get gets the item associated to the specified ID; otherwise, false.
func (fds FileDefs) Get(defID string) (FileDef, bool) {
	fd, ok := fds.defs[defID]

	return fd, ok
}

// SrcDefs gets all the items that are source csv data files.
func (fds FileDefs) SrcDefs() []FileDef {
	rv := make([]FileDef, 0, len(fds.defs))
	for _, v := range fds.defs {
		if v.Target().isSrc {
			rv = append(rv, v)
		}
	}

	return rv
}

func (fds FileDefs) add(fd FileDef) {
	if id := fd.ID(); !fds.Contains(id) {
		fds.defs[id] = fd
	}
}

func createFileDefs() FileDefs {
	return FileDefs{
		defs: make(map[string]FileDef),
	}
}
