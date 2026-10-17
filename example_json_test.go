package byterate_test

import (
	"encoding/json"
	"fmt"

	"github.com/djylb/byterate"
)

// savedRate stores a Rate in a configuration file or an API response in the
// application's own JSON form: the limit is restored, and the measured
// throughput is reported but not read back, as it is measured anew.
type savedRate struct {
	*byterate.Rate
}

type savedRateJSON struct {
	Limit      int64 `json:"limit"`      // bytes/s, 0 for none
	Throughput int64 `json:"throughput"` // bytes/s, measured
}

func (r *savedRate) MarshalJSON() ([]byte, error) {
	return json.Marshal(savedRateJSON{Limit: max(r.Limit(), 0), Throughput: r.Throughput()})
}

func (r *savedRate) UnmarshalJSON(b []byte) error {
	var v savedRateJSON
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	if r.Rate == nil {
		r.Rate = byterate.NewRate(v.Limit)
	} else {
		r.SetLimit(v.Limit)
	}
	return nil
}

// Keep a Rate in JSON. The package leaves the JSON form to the
// application, since what is stored, and under which names, is the
// application's format. The field is a pointer, so that encoding/json finds
// the methods also when the struct around it is not addressable.
func ExampleRate_json() {
	var user struct {
		Name string     `json:"name"`
		Rate *savedRate `json:"rate"`
	}
	_ = json.Unmarshal([]byte(`{"name":"alice","rate":{"limit":1048576}}`), &user)
	fmt.Println(user.Rate.Limit())
	out, _ := json.Marshal(user)
	fmt.Println(string(out))
	// Output:
	// 1048576
	// {"name":"alice","rate":{"limit":1048576,"throughput":0}}
}
