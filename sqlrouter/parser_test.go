package sqlrouter

import (
	"testing"
)

func TestExtractFirstIdentifier(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"t_order", "t_order"},
		{"`t_order`", "t_order"},
		{"demo_db.t_order", "t_order"},
		{"`demo_db`.`t_order`", "t_order"},
		{"demo_db.`t_order`", "t_order"},
		{"`demo_db`.t_order", "t_order"},
		{"  `demo_db`.`t_order`  ", "t_order"},
	}

	for _, tt := range tests {
		result := extractFirstIdentifier(tt.input)
		if result != tt.expected {
			t.Errorf("extractFirstIdentifier(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestRewriteTableInSQL(t *testing.T) {
	tests := []struct {
		sql         string
		newTable    string
		expected    string
	}{
		{
			sql:      "SELECT * FROM `demo_db`.`t_order` LIMIT 0,1000",
			newTable: "t_order_0",
			expected: "SELECT * FROM `t_order_0` LIMIT 0,1000",
		},
		{
			sql:      "SELECT * FROM demo_db.t_order WHERE id = 1",
			newTable: "t_order_2",
			expected: "SELECT * FROM `t_order_2` WHERE id = 1",
		},
		{
			sql:      "SELECT * FROM `t_order` WHERE id = 1",
			newTable: "t_order_1",
			expected: "SELECT * FROM `t_order_1` WHERE id = 1",
		},
	}

	for _, tt := range tests {
		result := RewriteTableInSQL(tt.sql, tt.newTable)
		if result != tt.expected {
			t.Errorf("RewriteTableInSQL(%q, %q) = %q, want %q", tt.sql, tt.newTable, result, tt.expected)
		}
	}
}
