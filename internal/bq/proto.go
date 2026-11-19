package bq

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// BigQuery CDC pseudo-columns for the Storage Write API.
const (
	ChangeTypeColumn     = "_CHANGE_TYPE"
	ChangeSequenceColumn = "_CHANGE_SEQUENCE_NUMBER"

	ChangeUpsert = "UPSERT"
	ChangeDelete = "DELETE"
)

// RowCodec encodes NDJSON-shaped rows (as produced by schema.Entity.Map) to
// the protobuf wire format the Storage Write API expects, for one table
// schema. Its descriptor is self-contained (nested messages only), as
// required by managedwriter.WithSchemaDescriptor.
type RowCodec struct {
	Schema     bigquery.Schema
	Descriptor *descriptorpb.DescriptorProto
	msg        protoreflect.MessageDescriptor
	cdc        bool
}

// NewRowCodec builds a codec for sch. With cdc the message also carries the
// _CHANGE_TYPE and _CHANGE_SEQUENCE_NUMBER pseudo-columns.
func NewRowCodec(sch bigquery.Schema, cdc bool) (*RowCodec, error) {
	root := &descriptorpb.DescriptorProto{Name: proto.String("Row")}
	if err := addFields(root, sch); err != nil {
		return nil, err
	}
	if cdc {
		n := int32(len(root.Field))
		for i, name := range []string{ChangeTypeColumn, ChangeSequenceColumn} {
			root.Field = append(root.Field, &descriptorpb.FieldDescriptorProto{
				Name: proto.String(name), Number: proto.Int32(n + int32(i) + 1),
				Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			})
		}
	}
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("qbo_row.proto"), Syntax: proto.String("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{root},
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("build row descriptor: %w", err)
	}
	return &RowCodec{Schema: sch, Descriptor: root, msg: fd.Messages().Get(0), cdc: cdc}, nil
}

func addFields(msg *descriptorpb.DescriptorProto, sch bigquery.Schema) error {
	for i, f := range sch {
		if !identRe.MatchString(f.Name) {
			return fmt.Errorf("unsafe column name %q", f.Name)
		}
		fdp := &descriptorpb.FieldDescriptorProto{
			Name: proto.String(f.Name), Number: proto.Int32(int32(i + 1)),
			Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		}
		if f.Repeated {
			fdp.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
		}
		switch f.Type {
		case bigquery.StringFieldType, bigquery.NumericFieldType, bigquery.JSONFieldType:
			fdp.Type = descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
		case bigquery.DateFieldType:
			fdp.Type = descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum() // days since epoch
		case bigquery.TimestampFieldType, bigquery.IntegerFieldType:
			fdp.Type = descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum() // micros since epoch / value
		case bigquery.BooleanFieldType:
			fdp.Type = descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()
		case bigquery.RecordFieldType:
			nested := &descriptorpb.DescriptorProto{Name: proto.String(fmt.Sprintf("M%d_%s", i+1, f.Name))}
			if err := addFields(nested, f.Schema); err != nil {
				return err
			}
			msg.NestedType = append(msg.NestedType, nested)
			fdp.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
			fdp.TypeName = proto.String(nested.GetName()) // resolved relative to msg
		default:
			return fmt.Errorf("column %s: unsupported type %s", f.Name, f.Type)
		}
		msg.Field = append(msg.Field, fdp)
	}
	return nil
}

// Encode serialises one row. changeType/sequence are only used for CDC
// codecs; an empty sequence leaves BigQuery's ingestion-order default.
func (c *RowCodec) Encode(row map[string]any, changeType, sequence string) ([]byte, error) {
	m := dynamicpb.NewMessage(c.msg)
	if err := fill(m, c.Schema, row, ""); err != nil {
		return nil, err
	}
	if c.cdc {
		fs := c.msg.Fields()
		m.Set(fs.ByName(ChangeTypeColumn), protoreflect.ValueOfString(changeType))
		if sequence != "" {
			m.Set(fs.ByName(ChangeSequenceColumn), protoreflect.ValueOfString(sequence))
		}
	}
	return proto.Marshal(m)
}

func fill(m *dynamicpb.Message, sch bigquery.Schema, row map[string]any, path string) error {
	fields := m.Descriptor().Fields()
	for _, f := range sch {
		v, ok := row[f.Name]
		if !ok || v == nil {
			continue
		}
		fd := fields.ByName(protoreflect.Name(f.Name))
		if !f.Repeated {
			pv, err := scalar(m, fd, f, v, path+f.Name)
			if err != nil {
				return err
			}
			m.Set(fd, pv)
			continue
		}
		arr, ok := v.([]any)
		if !ok {
			return fmt.Errorf("%s%s: expected an array, got %T", path, f.Name, v)
		}
		list := m.Mutable(fd).List()
		for i, el := range arr {
			if el == nil {
				continue
			}
			pv, err := element(list, fd, f, el, fmt.Sprintf("%s%s[%d]", path, f.Name, i))
			if err != nil {
				return err
			}
			list.Append(pv)
		}
	}
	return nil
}

func element(list protoreflect.List, fd protoreflect.FieldDescriptor, f *bigquery.FieldSchema, v any, path string) (protoreflect.Value, error) {
	if f.Type == bigquery.RecordFieldType {
		obj, ok := v.(map[string]any)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("%s: expected an object, got %T", path, v)
		}
		sub := list.NewElement().Message().Interface().(*dynamicpb.Message)
		if err := fill(sub, f.Schema, obj, path+"."); err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfMessage(sub), nil
	}
	return scalarValue(f.Type, v, path)
}

func scalar(m *dynamicpb.Message, fd protoreflect.FieldDescriptor, f *bigquery.FieldSchema, v any, path string) (protoreflect.Value, error) {
	if f.Type == bigquery.RecordFieldType {
		obj, ok := v.(map[string]any)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("%s: expected an object, got %T", path, v)
		}
		sub := m.NewField(fd).Message().Interface().(*dynamicpb.Message)
		if err := fill(sub, f.Schema, obj, path+"."); err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfMessage(sub), nil
	}
	return scalarValue(f.Type, v, path)
}

var epoch = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

func scalarValue(t bigquery.FieldType, v any, path string) (protoreflect.Value, error) {
	bad := func(err error) (protoreflect.Value, error) {
		return protoreflect.Value{}, fmt.Errorf("%s: %v (%T %v)", path, err, v, v)
	}
	switch t {
	case bigquery.StringFieldType, bigquery.NumericFieldType, bigquery.JSONFieldType:
		switch x := v.(type) {
		case string:
			return protoreflect.ValueOfString(x), nil
		case json.Number:
			return protoreflect.ValueOfString(x.String()), nil
		}
	case bigquery.BooleanFieldType:
		if b, ok := v.(bool); ok {
			return protoreflect.ValueOfBool(b), nil
		}
	case bigquery.IntegerFieldType:
		switch x := v.(type) {
		case int64:
			return protoreflect.ValueOfInt64(x), nil
		case int:
			return protoreflect.ValueOfInt64(int64(x)), nil
		case json.Number:
			i, err := strconv.ParseInt(x.String(), 10, 64)
			if err != nil {
				return bad(err)
			}
			return protoreflect.ValueOfInt64(i), nil
		}
	case bigquery.TimestampFieldType:
		switch x := v.(type) {
		case time.Time:
			return protoreflect.ValueOfInt64(x.UnixMicro()), nil
		case string:
			ts, err := parseTimestamp(x)
			if err != nil {
				return bad(err)
			}
			return protoreflect.ValueOfInt64(ts.UnixMicro()), nil
		}
	case bigquery.DateFieldType:
		if s, ok := v.(string); ok {
			d, err := time.Parse("2006-01-02", s)
			if err != nil {
				return bad(err)
			}
			return protoreflect.ValueOfInt32(int32(d.Sub(epoch).Hours() / 24)), nil
		}
	}
	return bad(fmt.Errorf("cannot encode as %s", t))
}

// SequenceNumber renders a _CHANGE_SEQUENCE_NUMBER that orders versions of a
// row by (updated_at, _extracted_at), mirroring the MERGE rule "update only
// when the staged version is not older". BigQuery compares each
// '/'-separated hex section as an unsigned integer.
func SequenceNumber(updatedAt, extractedAt time.Time) string {
	return strconv.FormatUint(uint64(max(updatedAt.UnixMicro(), 0)), 16) + "/" +
		strconv.FormatUint(uint64(max(extractedAt.UnixMicro(), 0)), 16)
}

// rowTime reads an NDJSON timestamp column.
func rowTime(row map[string]any, col string) time.Time {
	switch x := row[col].(type) {
	case string:
		t, _ := parseTimestamp(x)
		return t
	case time.Time:
		return x
	}
	return time.Time{}
}

// parseTimestamp accepts the mapper's RFC 3339 output and BigQuery's
// TO_JSON_STRING rendering ("2024-01-02T03:04:05.123456Z" or with a space).
func parseTimestamp(s string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999Z07"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognised timestamp %q", s)
}
