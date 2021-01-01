//
//  SqliteProviderTests.swift
//  SqlTests
//
//  Created by Neil Allain on 3/3/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import XCTest
@testable import Cql

class SqliteDriverTests: XCTestCase {

	private let dbName = "sqliteprovidertest.sqlite"
	
	private var tempDir: URL {
		return FileManager.default.temporaryDirectory
	}
	private func cleanup() {
		let db = tempDir.appendingPathComponent(dbName)
		if (FileManager.default.fileExists(atPath: db.relativePath)) {
			try! FileManager.default.removeItem(at: db)
		}
	}
	
	private func openTestDatabase() throws -> SqliteDriver {
		return try openTestDatabase(tables: [TableBuilder.build { SqliteTestObj() }])
	}
	private func openTestDatabase(tables: [TableSchemaProtocol]) throws -> SqliteDriver {
		let db = try SqliteDriver.open(url: tempDir.appendingPathComponent(dbName)) as! SqliteDriver
		for schema in tables {
			let createSql = SqliteDriver.sqlForCreate(table: schema)
			try db.execute(sql: createSql, arguments: [])
		}
		return db
	}
	override func setUp() {
		cleanup()
	}
	
	override func tearDown() {
		cleanup()
	}
	
	func testOpen() {
		do {
			let _ = try SqliteDriver.open(url: tempDir.appendingPathComponent(dbName))
		} catch {
			XCTFail("failed with error: \(error.localizedDescription)")
		}
	}
	
	private func createSchema<T: Codable>(name: String, columns: [ColumnDefinition], primaryKey: String?, indexes: [TableIndex], prototypeRow: T) -> TableSchema<T> {
		let table = TableSchema<T>(name: name) { prototypeRow }
		for col in columns {
			table.add(column: col)
		}
		if let primaryKey = primaryKey {
			table.primaryKey = [primaryKey]
		}
		table.indexes = indexes
		return table
	}
	func testCrud() {
		do {
			let db = try openTestDatabase()
			let testDate = Date()
			let testUuid = UUID()
			let args: [SqlArgument] = [
				SqlArgument(name: "id", value: .int(5)),
				SqlArgument(name: "name", value: .text("abc")),
				SqlArgument(name: "num", value: .int(10)),
				SqlArgument(name: "date", value: .date(testDate)),
				SqlArgument(name: "uuid", value: .uuid(testUuid)),
				SqlArgument(name: "double", value: .real(3.14)),
				SqlArgument(name: "bool", value: .bool(true)),
			]
			// insert
			try db.execute(sql: "insert into SqliteTestObj (id, name, num, date, uuid, double, bool) values ({id}, {name}, {num}, {date}, {uuid}, {double}, {bool})"
				, arguments: args)
			
			// select
			do {
				let cursor = try db.query(sql: "select id, name, num, date, uuid, double, bool from SqliteTestObj",
																	bind: ["id", "name", "num", "date", "uuid", "double", "bool"],
																	arguments: [])
				let first = try cursor.next()
				XCTAssertNotNil(first)
				let n = try first?.getNullableInt(name: "id")
				XCTAssertEqual(5, n)
				XCTAssertEqual("abc", try first?.getNullableText(name: "name"))
				XCTAssertEqual(10, try first?.getNullableInt(name: "num"))
				XCTAssertEqual(testDate, try first?.getNullableDate(name: "date"))
				XCTAssertEqual(testUuid, try first?.getNullableUuid(name: "uuid"))
				XCTAssertEqual(3.14, try first?.getNullableReal(name: "double"))
				XCTAssertEqual(true, try first?.getNullableBool(name: "bool"))
				XCTAssertNil(try cursor.next())
			}
			// update
			do {
				try db.execute(sql: "update SqliteTestObj set name = {name} where id = {id}",
											 arguments: [SqlArgument(name: "name", value: .text("change")), SqlArgument(name: "id", value: .int(5))])
				let cursor = try db.query(sql: "select name from SqliteTestObj where id = {id}",
																		 bind: ["name"], arguments: [SqlArgument(name: "id", value: .int(5))])
				let reader = try cursor.next()
				XCTAssertNotNil(reader)
				XCTAssertEqual("change", try reader?.getNullableText(name: "name"))
				XCTAssertNil(try cursor.next())
			}
			// delete
			do {
				try db.execute(sql: "delete from SqliteTestObj where id = {id}", arguments: [SqlArgument(name: "id", value: .int(5))])
				let cursor = try db.query(sql: "select name from SqliteTestObj where id = {id}",
																	bind: ["name"], arguments: [SqlArgument(name: "id", value: .int(5))])
				XCTAssertNil(try cursor.next())
			}
		} catch {
			XCTFail("failed with exception: \(error.localizedDescription)")
		}
	}
	func testSqlTypes() {
		XCTAssertEqual("NUM", SqlType.int.sqliteType)
		XCTAssertEqual("NUM", SqlType.bool.sqliteType)
		XCTAssertEqual("", SqlType.blob.sqliteType)
		XCTAssertEqual("", SqlType.uuid.sqliteType)
		XCTAssertEqual("REAL", SqlType.real.sqliteType)
		XCTAssertEqual("REAL", SqlType.date.sqliteType)
		XCTAssertEqual("TEXT", SqlType.text.sqliteType)
	}
	func testCreateTableSql() {
		let schema = createSchema(name: "Stuff", columns: [
			ColumnDefinition(name: "name", sqlType: .text, defaultValue: SqlType.text.defaultValue),
			ColumnDefinition(name: "num", sqlType: .int, defaultValue: .null)
			],
			primaryKey: "name", indexes: [], prototypeRow: SqliteTestObj())
		let createSql = SqliteDriver.sqlForCreate(table: schema)
		XCTAssertEqual("CREATE TABLE Stuff(name TEXT default \'\' NOT NULL, num NUM default null, PRIMARY KEY (name)) WITHOUT ROWID;", createSql)
	}
	
	func testCreateIndexSql() {
		let index = TableIndex(columnNames: ["c", "d"], isUnique: false)
		XCTAssertEqual("CREATE INDEX t1_c_d on t1(c, d);", SqliteDriver.sqlForCreate(index: index, on: "t1"))
		let uniqueIndex = TableIndex(columnNames: ["a", "b"], isUnique: true)
		XCTAssertEqual("CREATE UNIQUE INDEX t2_a_b on t2(a, b);", SqliteDriver.sqlForCreate(index: uniqueIndex, on: "t2"))
	}
	
	func testGetSchema() {
		do {
			let schema = AllTable.buildSchema()
			let joinSchema = JoinTable.buildSchema()
			let childSchema = ChildTable.buildSchema()
			let sql = SqliteDriver.sqlForCreate(table: schema)
			let joinSql = SqliteDriver.sqlForCreate(table: joinSchema)
			let childSql = SqliteDriver.sqlForCreate(table: childSchema)
			let db = try openTestDatabase()
			try db.execute(sql: sql, arguments: [])
			try db.execute(sql: childSql, arguments: [])
			try db.execute(sql: joinSql, arguments: [])
			for index in childSchema.indexes {
				try db.execute(sql: SqliteDriver.sqlForCreate(index: index, on: childSchema.name), arguments: [])
			}
			let fullSchema = try db.getExistingTables()
			verifySchemaEqual(fullSchema, schema)
			verifySchemaEqual(fullSchema, childSchema)
			verifySchemaEqual(fullSchema, joinSchema)
		} catch {
			XCTFail(error.localizedDescription)
		}
	}

	func testMigrateTableChange() {
		let sqlDriver = autoMigrate(from: [OldModel.renamedSchema(to: NewModel.self)], to: [NewModel.buildSchema()], rename: []) { driver in
			try driver.execute(sql: "insert into NewModel (id, name, notes) values (1, 'my name', 'my notes')", arguments: [])
		}
		if let driver = sqlDriver {
			verifyData(driver: driver, table: "NewModel",
								 columns: [("id", .int), ("notes", .text)],
								 expectedData: [[.int(1), .text("my notes")]])
		}
	}

	func testMigrateTableAndColumnRename() {
		let sqlDriver = autoMigrate(
			from: [OldModelRename.buildSchema()],
			to: [NewModelRename.buildSchema()],
			rename: [.renamed(class: NewModelRename.self, from: "OldModelRename"), .renamed(property: \NewModelRename.fullName, from: "name")]) { driver in
			try driver.execute(sql: "insert into OldModelRename (id, name, notes) values (1, 'my name', 'my notes')", arguments: [])
		}
		if let driver = sqlDriver {
			verifyData(driver: driver, table: "NewModelRename",
								 columns: [("id", .int), ("fullName", .text), ("notes", .text)],
								 expectedData: [[.int(1), .text("my name"), .text("my notes")]])
		}

	}
	private func autoActions(from startingSchema: [TableSchemaProtocol], to expectedSchema: [TableSchemaProtocol], rename: [SchemaRefactor]) -> [MigrationAction] {
		let diffs = SchemaDifference.compare(differ: DatabaseProvider.sqlite, existing: startingSchema, expected: expectedSchema, refactors: rename)
		XCTAssertTrue(SchemaDifference.canAutoMigrate(diffs))
		return diffs.map { MigrationAction.auto($0) }
	}
	private func autoMigrate(from startSchema: [TableSchemaProtocol], to expectedSchema: [TableSchemaProtocol], rename: [SchemaRefactor], seedData: (SqliteDriver) throws -> Void) -> SqliteDriver? {
		return migrate(from: startSchema, to: expectedSchema, actions: autoActions(from: startSchema, to: expectedSchema, rename: rename), seedData: seedData)
	}
	private func migrate(from startingSchema: [TableSchemaProtocol], to expectedSchema: [TableSchemaProtocol], actions: [MigrationAction], seedData: (SqliteDriver) throws -> Void) -> SqliteDriver? {
		do {
			let driver = try openTestDatabase(tables: startingSchema)
			try seedData(driver)
			try driver.migrate(actions: actions)
			verifySchema(driver, expectedSchema)
			return driver
		} catch {
			let schemaDesc = expectedSchema.map({$0.description}).joined(separator: ", ")
			XCTFail("failed to migrate to schema: \(schemaDesc)")
		}
		return nil
	}
	private func verifySchema(_ driver: SqliteDriver, _ expectedSchema: [TableSchemaProtocol]) {
		do {
			let schema = try driver.getExistingTables()
			let diffs = SchemaDifference.compare(differ: DatabaseProvider.sqlite, existing: schema, expected: expectedSchema)
			let diffDesc = diffs.map({ $0.description }).joined(separator: ", ")
			XCTAssertTrue(diffs.isEmpty, "Schemas are different: \(diffDesc)")
		} catch {
			XCTFail("error attempting to verify schema")
		}
	}

	private func verifySchemaEqual(_ fullSchema: [TableSchemaProtocol], _ schema: TableSchemaProtocol) {
		let checkSchema = fullSchema.first { $0.name == schema.name }
		XCTAssertNotNil(checkSchema)
		if let checkSchema = checkSchema {
			let comparison = SchemaTableDifference.compare(differ: DatabaseProvider.sqlite, existing: checkSchema, expected: schema)
			XCTAssertEqual(0, comparison.count)
			for comp in comparison {
				print(comp)
			}
		}
	}
	private func verifyData(driver: SqliteDriver, table: String, columns: [(String, SqlType)], expectedData: [[SqlValue]]) {
		do {
			let colNames = columns.map({ $0.0 })
			var unfoundRows = expectedData
			let cursor = try driver.query(sql: "select \(colNames.joined(separator: ", ")) from \(table)", bind: colNames, arguments: [])
			while let reader = try cursor.next() {
				var row = [SqlValue]()
				for col in columns {
					let val = try reader.getSqlValue(name: col.0, type: col.1)
					row.append(val)
				}
				if let found = unfoundRows.firstIndex(where: {$0 == row}) {
					unfoundRows.remove(at: found)
				} else {
					XCTFail("unexpected row: \(row)")
				}
			}
			XCTAssertEqual(0, unfoundRows.count, "did not find row: \(unfoundRows)")
		} catch {
			XCTFail("error while verifying data: \(error.localizedDescription)")
		}
	}
}

struct SqliteTestObj: Codable {
	var id: Int = 0
	var name: String = ""
	var num: Int = 0
	var date: Date = Date()
	var uuid: UUID = UUID()
	var double: Double = 0
	var bool = false
}

extension SqlReader {
	func getSqlValue(name: String, type: SqlType) throws -> SqlValue {
		switch type {
		case .blob:
			let v = try getNullableBlob(name: name)
			return v != nil ? .data(v!) : .null
		case .bool:
			let v = try getNullableBool(name: name)
			return v != nil ? .bool(v!) : .null
		case .date:
			let v = try getNullableDate(name: name)
			return v != nil ? .date(v!) : .null
		case .int:
			let v = try getNullableInt(name: name)
			return v != nil ? .int(v!) : .null
		case .real:
			let v = try getNullableReal(name: name)
			return v != nil ? .real(v!) : .null
		case .text:
			let v = try getNullableText(name: name)
			return v != nil ? .text(v!) : .null
		case .uuid:
			let v = try getNullableUuid(name: name)
			return v != nil ? .uuid(v!) : .null
		}
	}

}
//struct NotCreatedObj: Codable {
//	var id: Int = 0
//}
//
//struct DiffObj: Codable {
//	var
//}

