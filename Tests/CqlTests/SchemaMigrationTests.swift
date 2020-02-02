//
//  SchemaMigrationTests.swift
//  SqlTests
//
//  Created by Neil Allain on 5/16/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import XCTest
@testable import Cql

class SchemaMigrationTests: XCTestCase {
	
	override func setUp() {
		// Put setup code here. This method is called before the invocation of each test method in the class.
	}
	
	override func tearDown() {
		// Put teardown code here. This method is called after the invocation of each test method in the class.
	}
	
	func testCompare() {
		let comparison = compare(OldModel.self, NewModel.self)
		XCTAssertEqual(4, comparison.count)
		verifyDiff(.newColumn(ColumnDefinition(name: "other", sqlType: .uuid, defaultValue: SqlType.uuid.defaultValue)), comparison)
		verifyDiff(.removedColumn(ColumnDefinition(name: "name", sqlType: .text, defaultValue: SqlType.text.defaultValue)), comparison)
		verifyDiff(.newIndex(TableIndex(columnNames: ["other"], isUnique: false)), comparison)
		verifyDiff(.removedIndex(TableIndex(columnNames: ["name"], isUnique: true)), comparison)
	}
	func testCompareJoin() {
		let comparison = compare(OldJoinModel.self, NewJoinModel.self)
		XCTAssertEqual(2, comparison.count)
		verifyDiff(.newForeignKey(ForeignKey(columnName: "parentId", foreignTable: "NewModel", foreignColumn: "id")), comparison)
		verifyDiff(.removedForeignKey(ForeignKey(columnName: "parentId", foreignTable: "OldModel", foreignColumn: "id")), comparison)
	}
	func testTableRename() {
		let old = NewModel.renamedSchema(to: OldModel.self)
		let new = NewModel.buildSchema()
		let comparison = SchemaDifference.compare(
			differ: DatabaseProvider.sqlite,
			existing: [old],
			expected: [new],
			refactors: [.renamed(class: NewModel.self, from: "OldModel")])
		XCTAssertEqual(1, comparison.count)
		if let tableDiff = comparison.first {
			switch tableDiff {
			case .changedTable(let o, let n, let diffs):
				XCTAssertEqual(old.name, o.name)
				XCTAssertEqual(new.name, n.name)
				XCTAssertEqual(0, diffs.count)
			default:
				XCTFail("wrong difference: \(tableDiff)")
			}
		}
	}
	
	func testColumnRename() {
		let old = OldModelRename.renamedSchema(to: NewModelRename.self)
		let new = NewModelRename.buildSchema()
		let comparison = SchemaDifference.compare(
			differ: DatabaseProvider.sqlite,
			existing: [old],
			expected: [new],
			refactors: [.renamed(property: \NewModelRename.fullName, from: "name")])
		let oldCol = old.columns.first { $0.name == "name" }
		let newCol = new.columns.first { $0.name == "fullName" }
		XCTAssertEqual(1, comparison.count)
		if let tableDiff = comparison.first {
			switch tableDiff {
			case .changedTable(let o, let n, let diffs):
				XCTAssertEqual(old.name, o.name)
				XCTAssertEqual(new.name, n.name)
				XCTAssertEqual(1, diffs.count)
				verifyDiff(.renamedColumn(from: oldCol!, to: newCol!), diffs)
			default:
				XCTFail("wrong difference: \(tableDiff)")
			}
		}
	}
	func testDefaultChange() {
		let comparison = compare(OldDefaultChange.self, NewDefaultChange.self)
		XCTAssertEqual(1, comparison.count)
		verifyDiff(.changedDefault(
			from: ColumnDefinition(name: "description", sqlType: .text, defaultValue: .null),
			to: ColumnDefinition(name: "description", sqlType: .text, defaultValue: SqlType.text.defaultValue)), comparison)
	}
	func testPrimaryKeyChange() {
		let comparison = compare(OldPrimaryKeyChange.self, NewPrimaryKeyChange.self)
		XCTAssertEqual(1, comparison.count)
		let o = OldPrimaryKeyChange.buildSchema().primaryKeyColumns
		let n = NewPrimaryKeyChange.buildSchema().primaryKeyColumns
		verifyDiff(.changedPrimaryKey(from: o, to: n), comparison)
	}
	func testColumnMappings() {
		let comparison = compare(OldModel.self, NewModel.self)
		let oldSchema = OldModel.buildSchema()
		let newSchema = NewModel.buildSchema()
		let columnMappings = SchemaTableDifference.columnMappings(source: oldSchema, target: newSchema, differences: comparison)
		verifyColumnMapping(source: oldSchema, target: newSchema, expected: ("id", "id"), mappings: columnMappings)
		verifyColumnMapping(source: oldSchema, target: newSchema, expected: ("notes", "notes"), mappings: columnMappings)
	}
	func verifyColumnMapping(source: TableSchemaProtocol, target: TableSchemaProtocol, expected: (String, String), mappings: [(ColumnDefinition, ColumnDefinition)]) {
		let (sourceCol, targetCol) = expected
		if let (src, tgt) = mappings.first(where: { $0.0.name == sourceCol && $0.1.name == targetCol}) {
			XCTAssertEqual(src, source.columns.first(where: {$0.name == sourceCol} ))
			XCTAssertEqual(tgt, target.columns.first(where: {$0.name == targetCol} ))
		} else {
			XCTFail("no column mapping for \(expected)")
		}
	}
	func testSchemaNoChange() {
		let old = [OldModel.buildSchema(), OldJoinModel.buildSchema(), ChildModel.buildSchema()]
		let new = [OldModel.buildSchema(), OldJoinModel.buildSchema(), ChildModel.buildSchema()]
		let diffs = SchemaDifference.compare(differ: DatabaseProvider.sqlite, existing: old, expected: new)
		XCTAssertEqual(0, diffs.count)
	}
	func testSchemaAddedTable() {
		let old = [OldModel.buildSchema(), OldJoinModel.buildSchema(), ChildModel.buildSchema()]
		let new = [OldModel.buildSchema(), OldJoinModel.buildSchema(), ChildModel.buildSchema(), NewModel.buildSchema()]
		let diffs = SchemaDifference.compare(differ: DatabaseProvider.sqlite, existing: old, expected: new)
		XCTAssertEqual(1, diffs.count)
		XCTAssertEqual(SchemaDifference.newTable(NewModel.buildSchema()), diffs[0])
	}
	func testSchemaRemovedTable() {
		let new = [OldModel.buildSchema(), OldJoinModel.buildSchema(), ChildModel.buildSchema()]
		let old = [OldModel.buildSchema(), OldJoinModel.buildSchema(), ChildModel.buildSchema(), NewModel.buildSchema()]
		let diffs = SchemaDifference.compare(differ: DatabaseProvider.sqlite, existing: old, expected: new)
		XCTAssertEqual(1, diffs.count)
		XCTAssertEqual(SchemaDifference.removedTable(NewModel.buildSchema()), diffs[0])
	}
	func testSchemaChangedTable() {
		let old = OldModel.buildSchema()
		let new = NewModel.renamedSchema(to: OldModel.self)
		let diffs = SchemaDifference.compare(differ: DatabaseProvider.sqlite, existing: [old], expected: [new])
		XCTAssertEqual(1, diffs.count)
		switch diffs[0] {
		case .changedTable(_, _, let tableDiffs):
			XCTAssertEqual(SchemaTableDifference.compare(differ: DatabaseProvider.sqlite, existing: old, expected: new), tableDiffs)
		default:
			XCTFail("incorrect diff: \(diffs[0])")
		}
	}
	private func compare<Old: CqlTableRepresentable, New: CqlTableRepresentable>(_ oldSchema: Old.Type, _ newSchema: New.Type) -> [SchemaTableDifference] {
		return SchemaTableDifference.compare(differ: DatabaseProvider.sqlite, existing: oldSchema.buildSchema(), expected: newSchema.buildSchema())

	}
	private func verifyDiff(_ diff: SchemaTableDifference, _ diffs: [SchemaTableDifference]) {
		guard let _ = diffs.first(where: { $0 == diff }) else {
			XCTFail("could not find diff: \(diff)")
			return
		}
	}
}

