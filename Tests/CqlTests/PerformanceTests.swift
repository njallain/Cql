//
//  PerformanceTests.swift
//  SqlTests
//
//  Created by Neil Allain on 5/14/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import XCTest
@testable import Cql

class PerformanceTests: XCTestCase {
	
	private let dbName = "databasetest"
	
	private var tempDir: URL {
		return FileManager.default.temporaryDirectory
	}
	func cleanup() {
		let db = tempDir.appendingPathComponent(dbName + ".sqlite")
		if (FileManager.default.fileExists(atPath: db.relativePath)) {
			try! FileManager.default.removeItem(at: db)
		}
	}
	
	func openTestDatabase() throws -> Storage {
		let db = Database(name: dbName, location: tempDir, provider: .sqlite, version: "1", tables: [
			.table(SmallIntId.self),
			.table(SmallStringId.self),
			.table(SmallUuidId.self),
			.table(MediumIntId.self),
			.table(MediumIntIdCustom.self)
			])
		return db
	}
	
	override func setUp() {
		cleanup()
	}
	
	override func tearDown() {
		cleanup()
	}
	
	private func insertInt(conn: StorageConnection) throws {
		let id = try conn.nextId(SmallIntId.self)
		let row = SmallIntId(id: id, name: "row \(id)")
		try conn.insert(row)
	}
	private func insertTest(batches: Int, size: Int, insertFn: (StorageConnection) throws -> Void) {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			self.measure {
				do {
					for _ in 0 ..< batches {
						let txn = try conn.beginTransaction()
						for _ in 0 ..< size {
							try insertFn(conn)
						}
						try txn.commit()
					}
				} catch {
					XCTFail(error.localizedDescription)
				}
			}
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	private func insertTest(insertFn: (StorageConnection) throws -> Void) {
		insertTest(batches: 100, size: 100, insertFn: insertFn)
	}
	
	func testIntIds() {
		insertTest(insertFn: insertInt)
	}
	func testStringIds() {
		self.insertTest(insertFn: { conn in
			let id = UUID().uuidString
			let row = SmallStringId(id: id, name: id)
			try conn.insert(row)
		})
	}
	func testUuids() {
		self.insertTest(insertFn: { conn in
			let id = UUID()
			let row = SmallUuidId(id: id, name: id.uuidString)
			try conn.insert(row)
		})
	}
	
	func testMediumInsert() {
		self.insertTest(batches: 1, size: 1000) { conn in
			let m = createMedium(conn)
			try conn.insert(m)
		}
	}
	func testMediumCustomInsert() {
		self.insertTest(batches: 1, size: 1000) { conn in
			let m = createMediumCustom(conn)
			try conn.insert(m)
		}
	}
	func testRawSqliteInsert() {
		do {
			// init the database
			let db = try openTestDatabase()
			let conn = try db.open()
			let driver = try SqliteDriver.open(url: tempDir.appendingPathComponent(dbName + ".sqlite"))
			measure {
				do {
					let sql = """
							insert into MediumIntId (id, title, startDate, endDate, notes, position, priority)
							values ({id}, {title}, {startDate}, {endDate}, {notes}, {position}, {priority})
							"""
					try driver.beginTransaction()
					for _ in 1...1000 {
						let m = createMedium(conn)
						let b = ArgumentBuilder()
						b.add(name: "id", value: m.id)
						b.add(name: "title", value: m.title)
						b.add(name: "startDate", value: m.startDate)
						b.add(name: "endDate", value: m.endDate)
						b.add(name: "notes", value: m.notes)
						b.add(name: "position", value: m.position)
						b.add(name: "priority", value: m.priority)

//						let args = [SqlArgument(name: "id", value: .int(m.id)),
//												SqlArgument(name: "title", value: .text(m.title)),
//												SqlArgument(name: "startDate", value: .date(m.startDate)),
//												SqlArgument(name: "endDate", value: .null),
//												SqlArgument(name: "notes", value: .text(m.notes!)),
//												SqlArgument(name: "position", value: .real(m.position)),
//												SqlArgument(name: "priority", value: .int(m.priority))
//												]
						try driver.execute(sql: sql, arguments: b.values)
					}
					try driver.commitTransaction()
				} catch {
					XCTFail(error.localizedDescription)
				}
			}
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	
	func testMediumFetch() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let txn = try conn.beginTransaction()
			for _ in 1...1000 {
				try conn.insert(createMedium(conn))
			}
			try txn.commit()
			measure {
				do {
					let _ = try conn.find(Predicate.all(MediumIntId.self))
				} catch {
					XCTFail(error.localizedDescription)
				}
			}
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	func testMediumCustomFetch() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let txn = try conn.beginTransaction()
			for _ in 1...1000 {
				try conn.insert(createMediumCustom(conn))
			}
			try txn.commit()
			measure {
				do {
					let _ = try conn.find(Predicate.all(MediumIntIdCustom.self))
				} catch {
					XCTFail(error.localizedDescription)
				}
			}
		} catch {
			XCTFail(error.localizedDescription)
		}
	}

	func testRawSqliteFetch() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let txn = try conn.beginTransaction()
			for _ in 1...1000 {
				try conn.insert(createMedium(conn))
			}
			try txn.commit()
			let driver = try SqliteDriver.open(url: tempDir.appendingPathComponent(dbName + ".sqlite"))
			measure {
				do {
					let cols = ["id", "title", "startDate", "endDate", "notes", "position", "priority"]
					let sel = "select \(cols.joined(separator: ", ")) from MediumIntId"
					let cursor = try driver.query(sql: sel, bind: cols, arguments: [])
					var objs = [MediumIntId]()
					while let reader = try cursor.next() {
						var m = MediumIntId()
						m.id = try reader.getInt(name: "id")
						m.title = try reader.getText(name: "title")
						m.startDate = try reader.getDate(name: "startDate")
						m.endDate = try reader.getNullableDate(name: "endDate")
						m.notes = try reader.getNullableText(name: "notes")
						m.position = try reader.getReal(name: "position")
						m.priority = try reader.getInt(name: "priority")
						objs.append(m)
					}
				} catch {
					XCTFail(error.localizedDescription)
				}
			}
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	private func createMedium(_ conn: StorageConnection) -> MediumIntId {
		let i = try! conn.nextId(MediumIntId.self)
		return MediumIntId(
			id: i,
			title: "object \(i)",
			startDate: Date(),
			endDate: nil,
			notes: "object \(i) notes",
			position: Double(i),
			priority: i)
	}
	private func createMediumCustom(_ conn: StorageConnection) -> MediumIntIdCustom {
		let i = try! conn.nextId(MediumIntIdCustom.self)
		return MediumIntIdCustom(
			id: i,
			title: "object \(i)",
			startDate: Date(),
			endDate: nil,
			notes: "object \(i) notes",
			position: Double(i),
			priority: i)
	}
}

fileprivate struct SmallIntId: PrimaryKeyTable {
	var id: Int = 0
	var name: String = ""
	
	static let primaryKey = \SmallIntId.id
}

fileprivate struct MediumIntId: PrimaryKeyTable {
	var id: Int = 0
	var title: String = ""
	var startDate = Date(timeIntervalSinceReferenceDate: 0)
	var endDate: Date? = nil
	var notes: String? = nil
	var position: Double = 0
	var priority = 0
	
	static let primaryKey = \MediumIntId.id
}

fileprivate struct MediumIntIdCustom: PrimaryKeyTable {
	var id: Int = 0
	var title: String = ""
	var startDate = Date(timeIntervalSinceReferenceDate: 0)
	var endDate: Date? = nil
	var notes: String? = nil
	var position: Double = 0
	var priority = 0
	
	static let primaryKey = \MediumIntIdCustom.id
	private static func encode(_ object: MediumIntIdCustom, to builder: SqlBuilder) throws {
		builder.add(name: "id", value: object.id)
		builder.add(name: "title", value: object.title)
		builder.add(name: "startDate", value: object.startDate)
		builder.add(name: "endDate", value: object.endDate)
		builder.add(name: "notes", value: object.notes)
		builder.add(name: "position", value: object.position)
		builder.add(name: "priority", value: object.priority)
	}
	private static func decode(from reader: SqlReader, prefix: String) throws -> MediumIntIdCustom {
		let object = MediumIntIdCustom(
			id: try reader.getInt(name: prefix + "id"),
			title: try reader.getText(name: prefix + "title"),
			startDate: try reader.getDate(name: prefix + "startDate"),
			endDate: try reader.getNullableDate(name: prefix + "endDate"),
			notes: try reader.getNullableText(name: prefix + "notes"),
			position: try reader.getReal(name: prefix + "position"),
			priority: try reader.getInt(name: prefix + "priority")
		)
		return object
	}
	static let sqlCoder = SqlCoder<MediumIntIdCustom>(encode: MediumIntIdCustom.encode, decode: MediumIntIdCustom.decode)
}
fileprivate struct SmallStringId: PrimaryKeyTable {
	var id: String = ""
	var name: String = ""
	
	static let primaryKey = \SmallStringId.id
}

fileprivate struct SmallUuidId: PrimaryKeyTable {
	var id: UUID = UUID()
	var name: String = ""
	
	static let primaryKey = \SmallUuidId.id
}
