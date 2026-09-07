package connect

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// JSON is a jsonb column that stores a document without interpreting it.
// It exists so the shared models do not drag gorm.io/datatypes into every app.
type JSON json.RawMessage

func (d JSON) Value() (driver.Value, error) {
	if len(d) == 0 {
		return "{}", nil
	}
	if !json.Valid(d) {
		return nil, fmt.Errorf("invalid JSON document")
	}
	return string(d), nil
}

func (d *JSON) Scan(src any) error {
	if src == nil {
		*d = nil
		return nil
	}
	var raw []byte
	switch value := src.(type) {
	case []byte:
		raw = value
	case string:
		raw = []byte(value)
	default:
		return fmt.Errorf("scan JSON: unsupported type %T", src)
	}
	if !json.Valid(raw) {
		return fmt.Errorf("scan JSON: invalid JSON")
	}
	*d = append((*d)[:0], raw...)
	return nil
}

func (d JSON) MarshalJSON() ([]byte, error) {
	if len(d) == 0 {
		return []byte("null"), nil
	}
	return d, nil
}

func (d *JSON) UnmarshalJSON(data []byte) error {
	*d = append((*d)[:0], data...)
	return nil
}

// GormDataType makes AutoMigrate pick jsonb without a per-field tag.
func (JSON) GormDataType() string { return "jsonb" }

// EntryStatus mirrors enums.EntryStatus in every app: 0 active, 2 deleted.
const (
	EntryStatusActive  = 0
	EntryStatusDeleted = 2
)
