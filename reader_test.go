package chapointdat

import "testing"

func Test_Line_Unhandled_missing_leading_0(t *testing.T) {
	line := []byte("04638191C                      00140039INTERNATIONAL BEE RESEARCH ASSOCIATION<")
	r := NewReader()
	i := 1
	pt, ct := 0, 0
	err := r.line(line, i, &pt, &ct)
	if err != nil {
		t.Error(err)
	}
}
func Test_Line_Unhandled_variable_length_issue_missing_0(t *testing.T) {
	r := NewReader()
	i := 1
	pt, ct := 0, 0
	line := []byte("04638192201024407940002        19910915        NP25 3DZ194509          0093MR<HANS<KJAERSGAARD<<<<1 AGINCOURT STREET<<MONMOUTH<<WALES<MARKETING DIRECTOR<DANISH<ENGLAND<")
	err := r.line(line, i, &pt, &ct)
	if err != nil {
		t.Error(err)
	}
}

func Test_Line_InvalidCharacter(t *testing.T) {
	r := NewReader()
	i := 1
	pt, ct := 0, 0
	line := []byte("101222052301207115400002 20160413 WA11 RLÆ197908 0098MR<DAVID<SEOW<<<<840 IBIS COURT CENTRE PARK<<WARRINGTON<CHESHIRE<ENGLAND<DIRECTOR<BRITISH<ENGLAND<")
	err := r.line(line, i, &pt, &ct)
	if err == nil {
		t.Error("expected error")
	}
}

func Test_Company_Name(t *testing.T) {
	var name string
	tf := func(c Company) error {
		name = c.CompanyName
		return nil
	}
	r := NewReader(WithCompanyHandler(tf))
	i := 1
	pt, ct := 0, 0
	line := []byte("000000841D                      00000019A. WEST & PARTNERS<")
	err := r.line(line, i, &pt, &ct)
	if err != nil {
		t.Error(err)
	}
	expected := "A. WEST & PARTNERS"
	if name != expected {
		t.Errorf("incorrect name expected %s got %s", expected, name)
	}
}
