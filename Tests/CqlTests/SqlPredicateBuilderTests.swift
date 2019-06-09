//
//  SqlPredicateBuilderTests.swift
//  SqlTests
//
//  Created by Neil Allain on 4/14/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import XCTest
@testable import Cql

class SqlPredicateBuilderTests: XCTestCase {
	private let dbName = "databasetest"
	
	private var tempDir: URL {
		return FileManager.default.temporaryDirectory
	}
	private func cleanup() {
		let db = tempDir.appendingPathComponent(dbName + ".sqlite")
		if (FileManager.default.fileExists(atPath: db.relativePath)) {
			try! FileManager.default.removeItem(at: db)
		}
	}
	
	private func openTestDatabase() throws -> Database {
		let db = Database(name: dbName, location: tempDir, provider: .sqlite, version: "1", tables: [.table(PredTest.self), .table(Child.self)])
		return db
	}
	
	override func setUp() {
		cleanup()
	}
	
	override func tearDown() {
		cleanup()
	}
	
	func testSqlPredicate() {
		do {
			let db = try openTestDatabase()
			let pred = Where.all(PredTest.self)
				.property(\.id, .equal(5))
			let sqlBuilder = SqlPredicateCompiler<PredTest>(database: db)
			let sql = sqlBuilder.compile(pred)
			XCTAssertEqual("t0.id = {t0arg0}", sql.whereClause)
		} catch {
			XCTFail("\(error.localizedDescription)")
		}
	}
	
	func testIntEnumPredicate() {
		do {
			let db = try openTestDatabase()
			let pred = Where.all(PredTest.self)
				.property(\.nenum, .equal(.val1))
			let sqlBuilder = SqlPredicateCompiler<PredTest>(database: db)
			let sql = sqlBuilder.compile(pred)
			XCTAssertEqual("t0.nenum = {t0arg0}", sql.whereClause)
			XCTAssertEqual(1, sqlBuilder.arguments.count)
			XCTAssertEqual(SqlValue.int(IntEnum.val1.rawValue), sqlBuilder.arguments[0].value)
		} catch {
			XCTFail("\(error.localizedDescription)")
		}
	}
	
	func testParentSubPredicate() {
		do {
			let db = try openTestDatabase()
			let pred = Where.all(PredTest.self)
				.property(\.nenum, .equal(.val1))
			let childPred = Where.all(Child.self)
				.parent(Child.parent, pred)
			let sqlBuilder = SqlPredicateCompiler<Child>(database: db)
			let sql = sqlBuilder.compile(childPred)
			XCTAssertEqual("t0.parentId in (select t1.id from PredTest as t1 where t1.nenum = {t1arg0})", sql.whereClause)
			XCTAssertEqual(1, sqlBuilder.arguments.count)
			XCTAssertEqual(SqlValue.int(IntEnum.val1.rawValue), sqlBuilder.arguments[0].value)
		} catch {
			XCTFail("\(error.localizedDescription)")
		}
	}
	
	func testChildSubPredicate() {
		do {
			let db = try openTestDatabase()
			let childPred = Where.all(Child.self)
				.property(\.description, .equal("test"))
			let pred = Where.all(PredTest.self)
				.children(PredTest.children, childPred)
			let sqlBuilder = SqlPredicateCompiler<PredTest>(database: db)
			let sql = sqlBuilder.compile(pred)
			XCTAssertEqual("t0.id in (select t1.parentId from Child as t1 where t1.description = {t1arg0})", sql.whereClause)
			XCTAssertEqual(1, sqlBuilder.arguments.count)
			XCTAssertEqual(SqlValue.text("test"), sqlBuilder.arguments[0].value)
		} catch {
			XCTFail("\(error.localizedDescription)")
		}

	}
//	func testToManyJoin() {
//		do {
//			let db = try openTestDatabase()
//			let childrenPred = Where.all(Child.self)
//				.property(\.description, .equal("match"))
//			let pred = Where.all(PredTest.self)
//				.join(PredTest.children, childrenPred)
//			let compiler = SqlPredicateCompiler<PredTest>(database: db)
//			let sql = try compiler.compile(pred)
//			XCTAssertEqual("j0.description = {j0arg0}", sql.whereClause)
//			XCTAssertEqual("join Child as j0 on t1.id = j0.parentId", sql.joinClause)
//		} catch {
//			XCTFail("\(error.localizedDescription)")
//		}
//	}
}

fileprivate struct PredTest: PrimaryKeyTable {
	var id = 0
	var name = ""
	var nenum = IntEnum.val1
	var senum: StringEnum? = nil
	static let primaryKey = \PredTest.id
	static let children = toMany(\Child.parentId)
}

fileprivate struct Child: SqlTableRepresentable {
	var parentId = 0
	var description = ""
	
	static let parent = toOne(PredTest.self, \Child.parentId)
}
