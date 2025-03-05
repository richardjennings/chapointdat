# Companies House Appointment Data Reader

As documented at [chguide.co.uk](https://chguide.co.uk/bulk-data/officers/) and implemented with reference to a
[python](https://github.com/Global-Witness/uk-companies-house-parsers-public/blob/master/process_company_appointments_data.py#L37) 
version.

This is a couple of hours of work hacking something together currently.

Does not handle Person updates from 198.

Testing on a recent 195.zip shows occasional lines of the form:
```
04638191C                      00140039INTERNATIONAL BEE RESEARCH ASSOCIATION<
```
and 
```
04638192201024407940002        19910915        NP25 3DZ194509          0093MR<HANS<KJAERSGAARD<<<<1 AGINCOURT STREET<<MONMOUTH<<WALES<MARKETING DIRECTOR<DANISH<ENGLAND<
```

which both parse if an additional leading 0 is added - so that is what is 
currently done.

There are occasions in testing where the variable data length stated exceeds the
bounds of the current line being processed. If this occurs the code returns and
does not process the line any further.
