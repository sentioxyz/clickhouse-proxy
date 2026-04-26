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

func TestIsDatabaseWriter(t *testing.T) {
	owner := "0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa"
	writer := "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
	other := "0xCcCcCcCcCcCcCcCcCcCcCcCcCcCcCcCcCcCcCcCc"

	db := DatabaseInfo{DatabaseId: "foo", Owner: owner}
	perms := map[string]map[string]string{
		// Producer may store either case; helper compares case-insensitively.
		"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb": {"foo": writePermission},
		"0xdddddddddddddddddddddddddddddddddddddddd": {"foo": "read"},
	}

	tests := []struct {
		name string
		db   DatabaseInfo
		addr string
		want bool
	}{
		{"owner exact case", db, owner, true},
		{"owner lowercased", db, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true},
		{"writer via permissions (case-insensitive)", db, writer, true},
		{"non-writer with read permission", db, "0xDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD", false},
		{"unknown address", db, other, false},
		{"empty addr", db, "", false},
		{"empty owner, no perms", DatabaseInfo{DatabaseId: "foo"}, owner, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDatabaseWriter(tc.db, perms, tc.addr); got != tc.want {
				t.Errorf("isDatabaseWriter(%+v, %q) = %v, want %v", tc.db, tc.addr, got, tc.want)
			}
		})
	}
}
