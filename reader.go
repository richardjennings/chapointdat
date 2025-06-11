package chapointdat

import (
	"archive/zip"
	"bufio"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"strconv"
	"strings"
	"time"
)

const (
	companyRecordType        = "1"
	personRecordType         = "2"
	snapshotHeaderIdentifier = "DDDDSNAP"
	trailerRecordIdentifier  = "99999999"

	PrefixSC = Prefix("SC")
	PrefixSZ = Prefix("SZ")
	PrefixZC = Prefix("ZC")
	PrefixSF = Prefix("SF")
	PrefixFC = Prefix("FC")
	PrefixNI = Prefix("NI")
	PrefixNF = Prefix("NF")
	PrefixOC = Prefix("OC")
	PrefixSO = Prefix("SO")
	PrefixNC = Prefix("NC")
	PrefixSE = Prefix("SE")
	PrefixR  = Prefix("R")

	StatusC = Status("C")
	StatusD = Status("D")
	StatusL = Status("L")
	StatusR = Status("R")
)

type (
	Header struct {
		Run      int
		ProdDate time.Time
	}
	Footer struct {
		RecordCount int
	}
	Person struct {
		/*
		   The majority of company numbers are 8 digit numeric;
		   however, some consist of a prefix followed by digits.
		*/
		CompanyNumber,

		/*
		   This data item will contain one of the following values:
		   1. Appointment date taken from appointment document (includes 288a, AP01, AP02, AP03, AP04, RR01**, NI form
		      296, SEAP01, and SEAP02)
		   2  Appointment date taken from Annual Return (form 363)
		   3  Appointment date taken from incorporation document (includes form 10, IN01, NI form 21, SEFM01, SEFM02,
		      SEFM03, SEFM04, SEFM05, SECV01, and SETR02)
		   4  Appointment date taken from LLP appointment document (includes LLP288a, LLAP01, LLAP02, and NI form
		      LLP296a)
		   5  Appointment date taken from LLP incorporation document (includes LLP2, and LLIN01)
		   6  Appointment date taken from overseas company appointment document (includes BR4, OSAP01, OSAP02,
		      OSAP03, and OSAP04)
		   ** Appointment of secretary on re-registration from private company to PLC.
		*/
		AppDateOrigin,

		/*
		   current secretary  (00)
		   current director   (01)
		   resigned secretary  (02)
		   resigned director  (03)
		   current non-designated LLP Member  (04)
		   current designated LLP Member  (05)
		   resigned non-designated LLP Member (06)
		   resigned designated LLP Member (07)
		   current judicial factor  (11)
		   current receiver or manager appointed under the Charities Act  (12)
		   current manager appointed under the CAICE Act  (13)
		   resigned judicial factor  (14)
		   resigned receiver or manager appointed under the Charities Act  (15)
		   resigned manager appointed under the CAICE Act  (16)
		   current SE Member of Administrative Organ  (17)
		   current SE Member of Supervisory Organ  (18)
		   current SE Member of Management Organ  (19)
		   resigned SE Member of Administrative Organ  (20)
		   resigned SE Member of Supervisory Organ  (21)
		   resigned SE Member of Management Organ  (22)
		   errored appointment  (99)
		*/
		AppointmentType,

		/*
		   12 character numeric unique person identifier (increased from 10 characters).
		*/
		PersonNumber,

		/*
		   Will be set to “Y” if the officer is a corporate body, otherwise set to space.
		*/
		CorporateIndicator,
		/*
		   Will contain either spaces or an actual date in the format CCYYMMDD.  The value spaces will signify that
		   Companies House does not have an actual date for that item.
		   If an Appointment Date is provided for Appointment Type 11, 12, or 13 this refers to the date that the form
		   was registered; the actual date of appointment is not captured for these appointment types.
		*/
		AppointmentDate,

		/*
		   Will contain either spaces or an actual date in the format CCYYMMDD.  The value spaces will signify that
		   Companies House does not have an actual date for that item.
		   Resigned appointments are not normally included in a snapshot so this field will usually be blank.
		*/
		ResignationDate,

		/*
		   Current postcode for officer Service Address.
		*/
		Postcode,

		/*
		   Partial Date of Birth field will contain either all spaces, or a partial date of birth (century, year,
		   month) followed by 2 space characters in the format ‘CCYYMM  ‘.  If Full Date of Birth is provided then
		   Partial Date of Birth will also be provided.  However, Partial Date of Birth may be provided without Full
		   Date of Birth.
		*/
		PartialDateOfBirth,

		/*
		   Will contain either spaces or an actual date in the format CCYYMMDD.  The value spaces will signify that
		   Companies House does not have an actual date for that item.
		*/
		FullDateOfBirth,

		Title, Forenames, Surname,
		Honours, CareOf, PoBox, AddressLine1, AddressLine2, PostTown,
		County, Country, Occupation, Nationality, ResCountry string
	}
	Company struct {
		CompanyNumber,
		/*
		   “C”	  Converted/closed company
		   “D”	  Dissolved company
		   “L”	  Company in liquidation
		   “R”	  Company in receivership
		   Space  None of the above categories
		*/
		CompanyStatus,
		NumberOfOfficers,
		CompanyName string
	}
	Prefix string
	Status string
	Reader struct {
		personHandler  func(person Person) error
		companyHandler func(company Company) error
		headerHandler  func(header Header) error
		footerHandler  func(footer Footer) error
	}
	Opt func(r *Reader)
)

func WithPersonHandler(p func(person Person) error) Opt {
	return func(r *Reader) {
		r.personHandler = p
	}
}

func WithCompanyHandler(p func(company Company) error) Opt {
	return func(r *Reader) {
		r.companyHandler = p
	}
}

func WithHeaderHandler(p func(header Header) error) Opt {
	return func(r *Reader) {
		r.headerHandler = p
	}
}

func WithFooterHandler(p func(footer Footer) error) Opt {
	return func(r *Reader) {
		r.footerHandler = p
	}
}

func NewReader(opts ...Opt) *Reader {
	r := &Reader{
		personHandler:  func(p Person) error { return nil },
		companyHandler: func(c Company) error { return nil },
		headerHandler:  func(h Header) error { return nil },
		footerHandler:  func(f Footer) error { return nil },
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *Reader) Extract(path string, concurrency int, errH func(err error)) error {
	z, err := zip.OpenReader(path)
	if err != nil {
		return err
	}
	defer func() { _ = z.Close() }()

	for _, f := range z.File {
		var i, companiesProcessed, personsProcessed int
		zf, err := f.Open()
		if err != nil {
			return err
		}
		lineChan := make(chan []byte, concurrency*10)
		doneChan := make(chan bool)
		worker := func() error {
			for {
				select {
				case <-doneChan:
					for range concurrency - 1 {
						doneChan <- true
					}
					return nil

				case line := <-lineChan:
					if err := r.line(line, i, &personsProcessed, &companiesProcessed); err != nil {
						errH(fmt.Errorf("error: %w handling line: %s", err, string(line)))
					}
				}
			}
		}
		eg := errgroup.Group{}
		for range concurrency {
			eg.Go(worker)
		}
		scan := bufio.NewScanner(zf)
		for scan.Scan() {
			line := scan.Bytes()
			if err := r.line(line, i, &personsProcessed, &companiesProcessed); err != nil {
				errH(fmt.Errorf("error: %w handling line: %s", err, string(line)))
			}
			i++
		}
		doneChan <- true
		if err := eg.Wait(); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reader) line(line []byte, i int, pt, ct *int) error {
	if i == 0 {
		h, err := r.headerRow(line)
		if err != nil {
			return fmt.Errorf("error processing header row: %w", err)
		}
		if err := r.headerHandler(h); err != nil {
			return fmt.Errorf("error processing header handler: %w", err)
		}
	} else if trailerRecordIdentifier == string(line[0:8]) {
		recordCount, err := strconv.Atoi(strings.TrimSpace(string(line[8:16])))
		if err != nil {
			return fmt.Errorf("error processing trailer record row: %w", err)
		}
		if err := r.footerHandler(Footer{RecordCount: recordCount}); err != nil {
			return fmt.Errorf("error processing footer handler: %w", err)
		}
		if recordCount != *ct+*pt {
			return fmt.Errorf("unexpected number of records: %d", recordCount)
		}
	} else if string(line[8]) == companyRecordType {
		company, err := r.companyRow(line)
		if err != nil {
			return fmt.Errorf("error processing Company row: %w", err)
		}
		*ct++
		if err := r.companyHandler(company); err != nil {
			return fmt.Errorf("error processing Company handler: %w", err)
		}
	} else if string(line[8]) == personRecordType {
		person, err := r.personRow(line)
		if err != nil {
			return fmt.Errorf("error processing Person row: %w", err)
		}
		*pt++
		if err := r.personHandler(person); err != nil {
			return fmt.Errorf("error processing Person handler: %w", err)
		}
	} else {
		// sometimes it looks like leading 0's are missing
		if string(line[0]) == "0" {
			if string(line[1]) == "0" {
				return fmt.Errorf("unhandled record: %s", string(line))
			}
			line = append([]byte("0"), line...)
			return r.line(line, i, pt, ct)
		}
	}
	return nil
}

func (r Reader) headerRow(line []byte) (h Header, err error) {
	if string(line[0:8]) != snapshotHeaderIdentifier {
		err = errors.New("header line does not start with DDDDSNAP")
		return
	}
	run, err := strconv.Atoi(string(line[8:12]))
	if err != nil {
		err = fmt.Errorf("error reading run: %w", err)
		return
	}
	h.Run = run
	prodDate, err := time.Parse("20060102", string(line[12:20]))
	h.ProdDate = prodDate
	return
}

func (r Reader) personRow(line []byte) (p Person, err error) {
	p.CompanyNumber = strings.TrimSpace(string(line[0:8]))
	if strings.TrimSpace(string(line[8])) != personRecordType {
		err = errors.New("person row does not include personRecordType")
	}
	p.AppDateOrigin = strings.TrimSpace(string(line[9]))
	p.AppointmentType = strings.TrimSpace(string(line[10:12]))
	p.PersonNumber = strings.TrimSpace(string(line[12:24]))
	p.CorporateIndicator = strings.TrimSpace(string(line[24]))
	p.AppointmentDate = strings.TrimSpace(string(line[32:40]))
	p.ResignationDate = strings.TrimSpace(string(line[40:48]))
	p.Postcode = strings.TrimSpace(string(line[48:56]))
	p.PartialDateOfBirth = strings.TrimSpace(string(line[56:64]))
	p.FullDateOfBirth = strings.TrimSpace(string(line[64:72]))
	variableDataLength, err := strconv.Atoi(strings.TrimSpace(string(line[72:76])))
	if err != nil {
		// it seems like sometimes leading 0's are dropped, so lets add a 0 and
		// try again
		if string(line[0]) == "0" {
			if string(line[01]) == "0" {
				err = fmt.Errorf("error reading variable data length: %w", err)
				return
			}
			line = append([]byte("0"), line...)
			return r.personRow(line)
		}
	}
	variableData := line[76 : 76+variableDataLength]
	data := strings.Split(string(variableData), "<")
	if len(data) > 0 {
		p.Title = strings.TrimSpace(data[0])
	}
	if len(data) > 1 {
		p.Forenames = strings.TrimSpace(data[1])
	}
	if len(data) > 2 {
		p.Surname = strings.TrimSpace(data[2])
	}
	if len(data) > 3 {
		p.Honours = strings.TrimSpace(data[3])
	}
	if len(data) > 4 {
		p.CareOf = strings.TrimSpace(data[4])
	}
	if len(data) > 5 {
		p.PoBox = strings.TrimSpace(data[5])
	}
	if len(data) > 6 {
		p.AddressLine1 = strings.TrimSpace(data[6])
	}
	if len(data) > 7 {
		p.AddressLine2 = strings.TrimSpace(data[7])
	}
	if len(data) > 8 {
		p.PostTown = strings.TrimSpace(data[8])
	}
	if len(data) > 9 {
		p.County = strings.TrimSpace(data[9])
	}
	if len(data) > 10 {
		p.Country = strings.TrimSpace(data[10])
	}
	if len(data) > 11 {
		p.Occupation = strings.TrimSpace(data[11])
	}
	if len(data) > 12 {
		p.Nationality = strings.TrimSpace(data[12])
	}
	if len(data) == 14 {
		p.ResCountry = strings.TrimSpace(data[13])
	}
	return
}

func (r Reader) companyRow(line []byte) (c Company, err error) {
	c.CompanyNumber = strings.TrimSpace(string(line[0:8]))
	if string(line[8]) != companyRecordType {
		err = fmt.Errorf("company row does not include companyRecordType")
	}
	c.CompanyStatus = strings.TrimSpace(string(line[9]))
	c.NumberOfOfficers = strings.TrimSpace(string(line[32:36]))
	nameLength, err := strconv.Atoi(strings.TrimSpace(string(line[36:40])))
	if err != nil {
		err = fmt.Errorf("error reading name length: %w", err)
	}
	if nameLength+40 > len(line) {
		// hmmm
		return
	}
	c.CompanyName = strings.TrimSpace(string(line[40 : 40+nameLength-1]))
	return
}

func (s Status) String() string {
	switch s {
	case StatusC:
		return "Converted/closed company"
	case StatusD:
		return "Dissolved company"
	case StatusL:
		return "Company in liquidation"
	case StatusR:
		return "Company in receivership"
	default:
		return "Unknown"
	}
}

func (p Prefix) String() string {
	switch p {
	case PrefixSC:
		return "Company registered in Scotland"
	case PrefixSZ:
		return "Scottish company not required to register"
	case PrefixZC:
		return "English/Welsh company not required to register"
	case PrefixSF:
		return "Overseas Company registered in Scotland"
	case PrefixFC:
		return "Overseas Company registered in England/Wales (prior to 1st October 2009)" +
			"or Overseas Company registered in UK (from 1st October 2009)"
	case PrefixNI:
		return "Company registered in Northern Ireland"
	case PrefixNF:
		return "Overseas Company registered in Northern Ireland"
	case PrefixOC:
		return "Limited Liability Partnership registered in England/Wales"
	case PrefixSO:
		return "Limited Liability Partnership registered in Scotland"
	case PrefixNC:
		return "Limited Liability Partnership registered in Northern Ireland"
	case PrefixSE:
		return "Societas Europaea/UK Societas registered in England/Wales, Scotland or Northern Ireland"
	case PrefixR:
		return "Old company registered in Northern Ireland"
	default:
		return "Unknown"
	}
}

func (p Person) IsCorporate() bool {
	return p.CorporateIndicator == "Y"
}
