package chapointdat

import "testing"

func Test_Line_Unhandled_missing_leading_0(t *testing.T) {
	line := []byte("04638191C                      00140039INTERNATIONAL BEE RESEARCH ASSOCIATION<")
	r := NewReader(
		func(p Person) error {
			return nil
		},
		func(c Company) error {
			return nil
		},
		func(h Header) error {
			return nil
		},
		func(f Footer) error {
			return nil
		},
	)
	i := 1
	pt, ct := 0, 0
	err := r.line(line, i, &pt, &ct)
	if err != nil {
		t.Error(err)
	}
}
func Test_Line_Unhandled_variable_length_issue_missing_0(t *testing.T) {
	r := NewReader(
		func(p Person) error {
			return nil
		},
		func(c Company) error {
			return nil
		},
		func(h Header) error {
			return nil
		},
		func(f Footer) error {
			return nil
		},
	)
	i := 1
	pt, ct := 0, 0
	line := []byte("04638192201024407940002        19910915        NP25 3DZ194509          0093MR<HANS<KJAERSGAARD<<<<1 AGINCOURT STREET<<MONMOUTH<<WALES<MARKETING DIRECTOR<DANISH<ENGLAND<")
	err := r.line(line, i, &pt, &ct)
	if err != nil {
		t.Error(err)
	}
}
