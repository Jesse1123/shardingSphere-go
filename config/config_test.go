package config

import (
	"testing"
)

func TestParseActualDataNodes(t *testing.T) {
	tests := []struct {
		name            string
		actualDataNodes string
		wantType        string // "DataNode" or "DataNodeRange"
		wantDataSource  string
		wantTable       string
	}{
		{
			name:            "fixed ds with table range",
			actualDataNodes: "ds_0.t_order_${0..3}",
			wantType:        "DataNodeRange",
			wantDataSource:  "ds_0",
			wantTable:       "t_order_",
		},
		{
			name:            "single data node",
			actualDataNodes: "ds_0.t_user",
			wantType:        "DataNode",
			wantDataSource:  "ds_0",
			wantTable:       "t_user",
		},
		{
			name:            "ds range with table range",
			actualDataNodes: "ds_${0..1}.t_order_${0..3}",
			wantType:        "DataNodeRange",
			wantDataSource:  "ds_",
			wantTable:       "t_order_",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseActualDataNodes(tt.actualDataNodes)
			if err != nil {
				t.Fatalf("ParseActualDataNodes(%q) error: %v", tt.actualDataNodes, err)
			}

			switch v := result.(type) {
			case DataNode:
				if tt.wantType != "DataNode" {
					t.Errorf("expected DataNodeRange, got DataNode")
				}
				if v.DataSource != tt.wantDataSource {
					t.Errorf("DataSource = %q, want %q", v.DataSource, tt.wantDataSource)
				}
				if v.Table != tt.wantTable {
					t.Errorf("Table = %q, want %q", v.Table, tt.wantTable)
				}
			case *DataNodeRange:
				if tt.wantType != "DataNodeRange" {
					t.Errorf("expected DataNode, got DataNodeRange")
				}
				if v.DataSourcePattern != tt.wantDataSource {
					t.Errorf("DataSourcePattern = %q, want %q", v.DataSourcePattern, tt.wantDataSource)
				}
				if v.TablePattern != tt.wantTable {
					t.Errorf("TablePattern = %q, want %q", v.TablePattern, tt.wantTable)
				}
			default:
				t.Errorf("unknown type: %T", result)
			}
		})
	}
}
