package proxy

import "testing"

func TestIsCreateDatabase(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantName string
		wantOK   bool
	}{
		{"simple", "CREATE DATABASE foo", "foo", true},
		{"lowercase", "create database foo", "foo", true},
		{"mixed case", "Create Database foo_bar", "foo_bar", true},
		{"trailing semicolon", "CREATE DATABASE foo;", "foo", true},
		{"surrounding whitespace", "  \n CREATE DATABASE foo \n ", "foo", true},
		{"backtick identifier", "CREATE DATABASE `foo`", "foo", true},
		{"digit in name", "CREATE DATABASE db_v1", "db_v1", true},
		{"extra spaces between keywords", "CREATE  DATABASE   foo", "foo", true},

		{"not CREATE DATABASE", "CREATE TABLE foo (x Int)", "", false},
		{"missing name", "CREATE DATABASE", "", false},
		{"dashed identifier rejected", "CREATE DATABASE foo-bar", "", false},
		{"IF NOT EXISTS not supported yet", "CREATE DATABASE IF NOT EXISTS foo", "", false},
		{"ENGINE clause not supported", "CREATE DATABASE foo ENGINE=Atomic", "", false},
		{"dotted name rejected", "CREATE DATABASE foo.bar", "", false},
		{"select is not create database", "SELECT * FROM foo", "", false},
		{"drop is not create database", "DROP DATABASE foo", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := isCreateDatabase(tc.sql)
			if ok != tc.wantOK || got != tc.wantName {
				t.Errorf("isCreateDatabase(%q) = (%q, %v), want (%q, %v)", tc.sql, got, ok, tc.wantName, tc.wantOK)
			}
		})
	}
}
