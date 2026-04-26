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

func TestIsDropDatabase(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantName string
		wantOK   bool
	}{
		{"simple", "DROP DATABASE foo", "foo", true},
		{"lowercase", "drop database foo", "foo", true},
		{"mixed case", "Drop Database foo_bar", "foo_bar", true},
		{"trailing semicolon", "DROP DATABASE foo;", "foo", true},
		{"surrounding whitespace", "  \n DROP DATABASE foo \n ", "foo", true},
		{"backtick identifier", "DROP DATABASE `foo`", "foo", true},
		{"digit in name", "DROP DATABASE db_v1", "db_v1", true},
		{"extra spaces between keywords", "DROP  DATABASE   foo", "foo", true},

		{"not DROP DATABASE", "DROP TABLE foo", "", false},
		{"missing name", "DROP DATABASE", "", false},
		{"dashed identifier rejected", "DROP DATABASE foo-bar", "", false},
		{"IF EXISTS not supported yet", "DROP DATABASE IF EXISTS foo", "", false},
		{"ON CLUSTER not supported", "DROP DATABASE foo ON CLUSTER c", "", false},
		{"SYNC not supported", "DROP DATABASE foo SYNC", "", false},
		{"dotted name rejected", "DROP DATABASE foo.bar", "", false},
		{"create is not drop", "CREATE DATABASE foo", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := isDropDatabase(tc.sql)
			if ok != tc.wantOK || got != tc.wantName {
				t.Errorf("isDropDatabase(%q) = (%q, %v), want (%q, %v)", tc.sql, got, ok, tc.wantName, tc.wantOK)
			}
		})
	}
}

// state.IsDatabaseWriter is unit-tested in sentio-core
// (network/state/auth_test.go); the proxy's DROP DATABASE intercept
// trusts that helper for owner / write-permission semantics.
