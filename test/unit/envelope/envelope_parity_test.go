package envelope_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
)

// TestEnvelopeFieldParity ensures that domain.Envelope and events.Envelope have
// identical exported fields and JSON tags. Any divergence here means field
// values will be silently dropped in the publicToDomain / domainToPublic
// conversion functions — this test catches that at CI time.
func TestEnvelopeFieldParity(t *testing.T) {
	pubType := reflect.TypeOf(events.Envelope[json.RawMessage]{})
	domType := reflect.TypeOf(domain.Envelope[json.RawMessage]{})

	pubFields := make(map[string]reflect.StructField, pubType.NumField())
	for i := 0; i < pubType.NumField(); i++ {
		f := pubType.Field(i)
		pubFields[f.Name] = f
	}

	for i := 0; i < domType.NumField(); i++ {
		f := domType.Field(i)
		pubField, ok := pubFields[f.Name]
		if !ok {
			t.Errorf("domain.Envelope has field %q that is absent in events.Envelope — add it to both and update the conversion functions", f.Name)
			continue
		}
		if pubField.Tag != f.Tag {
			t.Errorf("field %q: domain JSON tag %q != events JSON tag %q — tags must match for round-trip correctness", f.Name, f.Tag, pubField.Tag)
		}
	}

	for name := range pubFields {
		found := false
		for i := 0; i < domType.NumField(); i++ {
			if domType.Field(i).Name == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("events.Envelope has field %q that is absent in domain.Envelope — add it to both and update the conversion functions", name)
		}
	}
}
