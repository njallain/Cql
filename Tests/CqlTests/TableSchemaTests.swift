//
//  TableSchemaTests.swift
//  SqlTests
//
//  Created by Neil Allain on 3/2/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import XCTest
@testable import Cql

class TableSchemaTests: XCTestCase {
	
	override func setUp() {
	}
	
	override func tearDown() {
		// Put teardown code here. This method is called after the invocation of each test method in the class.
	}
	
	func testBuildSchema() {
		let schema = AllTable.buildSchema()
		XCTAssertEqual("AllTable", schema.name)
		verifyColumn(schema: schema, name: "id", type: .uuid, nullable: false)
		verifyColumn(schema: schema, name: "nid", type: .uuid, nullable: true)
		verifyColumn(schema: schema, name: "n", type: .int, nullable: false)
		verifyColumn(schema: schema, name: "nn", type: .int, nullable: true)
		verifyColumn(schema: schema, name: "s", type: .text, nullable: false)
		verifyColumn(schema: schema, name: "ns", type: .text, nullable: true)
		verifyColumn(schema: schema, name: "dt", type: .date, nullable: false)
		verifyColumn(schema: schema, name: "ndt", type: .date, nullable: true)
		verifyColumn(schema: schema, name: "o", type: .blob, nullable: false)
		verifyColumn(schema: schema, name: "no", type: .blob, nullable: true)
		verifyColumn(schema: schema, name: "ia", type: .blob, nullable: false)
		verifyColumn(schema: schema, name: "nsa", type: .blob, nullable: true)
		verifyColumn(schema: schema, name: "oa", type: .blob, nullable: false)
		verifyColumn(schema: schema, name: "noa", type: .blob, nullable: true)
		verifyColumn(schema: schema, name: "b", type: .bool, nullable: false)
		verifyColumn(schema: schema, name: "nb", type: .bool, nullable: true)
		verifyColumn(schema: schema, name: "d", type: .real, nullable: false)
		verifyColumn(schema: schema, name: "nd", type: .real, nullable: true)
		verifyColumn(schema: schema, name: "se", type: .text, nullable: false)
		verifyColumn(schema: schema, name: "nse", type: .text, nullable: true)
		verifyColumn(schema: schema, name: "ie", type: .int, nullable: false)
		verifyColumn(schema: schema, name: "nie", type: .int, nullable: true)
		XCTAssertEqual(1, schema.primaryKey.count)
		XCTAssertEqual("id", schema.primaryKey[0])
	}
	
	func testIndexableTable() {
		let schema = IndexedObj.buildSchema()
		XCTAssertEqual(1, schema.primaryKey.count)
		XCTAssertEqual("id", schema.primaryKey[0])
		XCTAssertEqual(2, schema.indexes.count)
		XCTAssertEqual(["a"], schema.indexes[0].columnNames)
		XCTAssertEqual(true, schema.indexes[0].isUnique)
		XCTAssertEqual(["b", "c"], schema.indexes[1].columnNames)
		XCTAssertEqual(false, schema.indexes[1].isUnique)
	}
	
	func testForeignKeys() {
		let schema = FkObj.buildSchema()
		XCTAssertEqual(1, schema.foreignKeys.count)
		if let fk = schema.foreignKeys.first {
			XCTAssertEqual("indexedId", fk.columnName)
			XCTAssertEqual("Parent", fk.foreignTable)
			XCTAssertEqual("id", fk.foreignColumn)
		}
	}
	private func verifyColumn(schema: TableSchemaProtocol, name: String, type: SqlType, nullable: Bool) {
		guard let col = schema.columns.first(where: { $0.name == name}) else {
			XCTFail("column \(name) not found")
			return
		}
		XCTAssertEqual(type, col.sqlType, "column \(name) should be \(type) but is \(col.sqlType)")
		XCTAssertEqual(nullable, col.nullable, "column \(name) should be nullable: \(nullable)")
	}
	
}


fileprivate struct IndexedObj: Codable, SqlTable {
	var id: Int = 0
	var s: String = ""
	var a: Int = 0
	var b: Int = 0
	var c: Int = 0
	
	static let tableIndexes = [
		TableIndex(columnNames: ["a"], isUnique: true),
		TableIndex(columnNames: ["b","c"], isUnique: false)
	]
}

fileprivate struct DoubleKey: SqlTable {
	var id: Int = 0
	var id1: Int = 0
	var id2: String = ""
	var n: String = ""
	
}

fileprivate struct Parent: SqlTable {
	var id: Int = 0
	var name: String = ""
	
}

fileprivate struct FkObj: SqlTable {
	
	var id: Int = 0
	var t: String = ""
	var indexedId: Int = 0
	
	static let parent = toOne(Parent.self, \.indexedId)
	static let foreignKeys: [CqlForeignKeyRelation] = [parent]
}
