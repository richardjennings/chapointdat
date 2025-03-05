package chapointdat

import (
	"archive/zip"
	"bufio"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	companyRecordType        = "1"
	personRecordType         = "2"
	snapshotHeaderIdentifier = "DDDDSNAP"
	trailerRecordIdentifier  = "99999999"
)

type (
	Header struct {
		run      int
		prodDate time.Time
	}
	Footer struct {
		recordCount int
	}
	Person struct {
		companyNumber, appDateOrigin, appointmentType, personNumber,
		corporateIndicator, appointmentDate, resignationDate, postcode,
		partialDateOfBirth, fullDateOfBirth, title, forenames, surname,
		honours, careOf, poBox, addressLine1, addressLine2, postTown,
		county, country, occupation, nationality, resCountry string
	}
	Company struct {
		companyNumber, companyStatus, numberOfOfficers, companyName string
	}
	Reader struct {
		personHandler  func(person Person) error
		companyHandler func(company Company) error
		headerHandler  func(header Header) error
		footerHandler  func(footer Footer) error
	}
)

func NewReader(
	ph func(person Person) error,
	ch func(company Company) error,
	h func(header Header) error,
	f func(footer Footer) error,
) *Reader {
	return &Reader{
		personHandler:  ph,
		companyHandler: ch,
		headerHandler:  h,
		footerHandler:  f,
	}
}

func (r *Reader) Extract(path string) error {
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
		scan := bufio.NewScanner(zf)
		for scan.Scan() {
			line := scan.Bytes()
			if i == 0 {
				h, err := r.headerRow(line)
				if err != nil {
					return fmt.Errorf("error processing header row: %w", err)
				}
				if err := r.headerHandler(h); err != nil {
					return fmt.Errorf("error processing header handler: %w", err)
				}
			} else if trailerRecordIdentifier == string(line[0:8]) {
				recordCount, err := strconv.Atoi(string(line[8:16]))
				if err != nil {
					return fmt.Errorf("error processing trailer record row: %w", err)
				}
				if err := r.footerHandler(Footer{recordCount: recordCount}); err != nil {
					return fmt.Errorf("error processing footer handler: %w", err)
				}
				if recordCount != companiesProcessed+personsProcessed {
					return fmt.Errorf("unexpected number of records: %d", recordCount)
				}
			} else if string(line[8]) == companyRecordType {
				company, err := r.companyRow(line)
				if err != nil {
					return fmt.Errorf("error processing Company row: %w", err)
				}
				companiesProcessed++
				if err := r.companyHandler(company); err != nil {
					return fmt.Errorf("error processing Company handler: %w", err)
				}
			} else if string(line[8]) == personRecordType {
				person, err := r.personRow(line)
				if err != nil {
					return fmt.Errorf("error processing Person row: %w", err)
				}
				personsProcessed++
				if err := r.personHandler(person); err != nil {
					return fmt.Errorf("error processing Person handler: %w", err)
				}
			} else {
				return fmt.Errorf("unhandled record: %s", string(line))
			}
			i++
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
	h.run = run
	prodDate, err := time.Parse("20060102", string(line[12:20]))
	h.prodDate = prodDate
	return
}

func (r Reader) personRow(line []byte) (p Person, err error) {
	p.companyNumber = strings.TrimSpace(string(line[0:8]))
	if strings.TrimSpace(string(line[8])) != personRecordType {
		err = errors.New("Person row does not include personRecordType")
	}
	p.appDateOrigin = strings.TrimSpace(string(line[9]))
	p.appointmentType = strings.TrimSpace(string(line[10:12]))
	p.personNumber = strings.TrimSpace(string(line[12:24]))
	p.corporateIndicator = strings.TrimSpace(string(line[24]))
	p.appointmentDate = strings.TrimSpace(string(line[32:40]))
	p.resignationDate = strings.TrimSpace(string(line[40:48]))
	p.postcode = strings.TrimSpace(string(line[48:56]))
	p.partialDateOfBirth = strings.TrimSpace(string(line[56:64]))
	p.fullDateOfBirth = strings.TrimSpace(string(line[64:72]))
	variableDataLength, err := strconv.Atoi(strings.TrimSpace(string(line[72:76])))
	if err != nil {
		err = fmt.Errorf("error reading variable data length: %w", err)
	}
	variableData := line[76 : 76+variableDataLength]
	data := strings.Split(string(variableData), "<")
	if len(data) > 0 {
		p.title = strings.TrimSpace(data[0])
	}
	if len(data) > 1 {
		p.forenames = strings.TrimSpace(data[1])
	}
	if len(data) > 2 {
		p.surname = strings.TrimSpace(data[2])
	}
	if len(data) > 3 {
		p.honours = strings.TrimSpace(data[3])
	}
	if len(data) > 4 {
		p.careOf = strings.TrimSpace(data[4])
	}
	if len(data) > 5 {
		p.poBox = strings.TrimSpace(data[5])
	}
	if len(data) > 6 {
		p.addressLine1 = strings.TrimSpace(data[6])
	}
	if len(data) > 7 {
		p.addressLine2 = strings.TrimSpace(data[7])
	}
	if len(data) > 8 {
		p.postTown = strings.TrimSpace(data[8])
	}
	if len(data) > 9 {
		p.county = strings.TrimSpace(data[9])
	}
	if len(data) > 10 {
		p.country = strings.TrimSpace(data[10])
	}
	if len(data) > 11 {
		p.occupation = strings.TrimSpace(data[11])
	}
	if len(data) > 12 {
		p.nationality = strings.TrimSpace(data[12])
	}
	if len(data) == 14 {
		p.resCountry = strings.TrimSpace(data[13])
	}
	return
}

func (r Reader) companyRow(line []byte) (c Company, err error) {
	c.companyNumber = strings.TrimSpace(string(line[0:8]))
	if string(line[8]) != companyRecordType {
		err = fmt.Errorf("Company row does not include companyRecordType")
	}
	c.companyStatus = strings.TrimSpace(string(line[9]))
	c.numberOfOfficers = strings.TrimSpace(string(line[32:36]))
	nameLength, err := strconv.Atoi(string(line[36:40]))
	if err != nil {
		err = fmt.Errorf("error reading name length: %w", err)
	}
	c.companyName = strings.TrimSpace(string(line[40 : 40+nameLength]))
	return
}
