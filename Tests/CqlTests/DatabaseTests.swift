//
//  DatabaseTests.swift
//  SqlTests
//
//  Created by Neil Allain on 3/10/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import XCTest
@testable import Cql

class DatabaseTests: SqiliteTestCase {

	
	func openTestDatabase() throws -> Storage {
		return try openDatabase([
				.codable({SqliteTestObj()}),
				.codable({Foo()}),
				.table(FooChild.self),
				.table(KeyedFoo.self),
				.table(DoubleKeyed.self),
				.table(OptChild.self),
			])
	}
	

	func testInsert() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let row = Foo(id: 5, name: "test", description: "cde")
			try conn.insert(row)
			let results = try conn.find(Predicate(\Foo.id %== 5))
			XCTAssertEqual(1, results.count)
			verify(row, results)
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	
	func testInsertMultiple() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let rows = [
				Foo(id: 5, name: "test", description: "abc"),
				Foo(id: 6, name: "test2", description: nil)
			]
			try conn.insert(rows)
			let results = try conn.find(Predicate(all: Foo.self))
			XCTAssertEqual(2, results.count)
			verify(rows[0], results)
			verify(rows[1], results)
		} catch {
			XCTFail(error.localizedDescription)
		}

	}
	
	func testUpdate() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let row = Foo(id: 5, name: "test", description: "abc")
			let updatedRow = Foo(id: 6, name: "test2", description: nil)
			try conn.insert(row)
			try conn.update(where: Predicate(\Foo.id %== 5)) {
				$0.id = updatedRow.id
				$0.name = updatedRow.name
				$0.description = updatedRow.description
			}
			let results = try conn.find(Predicate(all: Foo.self))
			XCTAssertEqual(1, results.count)
			verify(updatedRow, results)
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	
	func testUpdateSingleKey() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			var o = KeyedFoo(id: 7, name: "my name", description: "desc", senum: .val1, nenum: nil)
			try conn.insert(o)
			o.name = "change"
			o.description = nil
			o.senum = .val2
			o.nenum = .val1
			try conn.update(o)
			guard let row = try conn.get(KeyedFoo.self, 7) else {
				XCTFail("couldn't find row")
				return
			}
			verify(o, row)
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	func testUpdateDoubleKey() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			var o = DoubleKeyed(leftId: 1, rightId: 2, name: "test")
			try conn.insert(o)
			o.name = "change"
			try conn.update(o)
			guard let row = try conn.get(DoubleKeyed.self, 1, 2) else {
				XCTFail("couldn't find row")
				return
			}
			verify(o, row)
		} catch {
			XCTFail(error.localizedDescription)
		}
	}

	func testDeleteDoubleKey() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let o = DoubleKeyed(leftId: 3, rightId: 4, name: "test")
			try conn.insert(o)
			try conn.delete(o)
			if let _ = try conn.get(DoubleKeyed.self, 3, 4) {
				XCTFail("row not deleted")
				return
			}
		} catch {
			XCTFail(error.localizedDescription)
		}
	}

	func testFindChildObjects() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let o = KeyedFoo(id: 7, name: "my name", description: "desc", senum: .val1, nenum: nil)
			let o2 = KeyedFoo(id: 8, name: "foo2", description: "desc", senum: .val2, nenum: .val2)
			let child1 = FooChild(fooId: 7, name: "first")
			let child2 = FooChild(fooId: 7, name: "second")
			let child3 = FooChild(fooId: 8, name: "another")
			let txn = try conn.beginTransaction()
			try conn.insert([o, o2])
			try conn.insert([child1, child2, child3])
			try txn.commit()
			let children = try conn.findRelated(KeyedFoo.children, of: o)
			XCTAssertEqual(2, children.count)
			verify(child1, children)
			verify(child2, children)
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	func testFindMultipleChildObjects() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let o = KeyedFoo(id: 7, name: "my name", description: "desc", senum: .val2, nenum: .val2)
			let o2 = KeyedFoo(id: 8, name: "foo2", description: "desc", senum: .val1, nenum: nil)
			let child1 = FooChild(fooId: 7, name: "first")
			let child2 = FooChild(fooId: 7, name: "second")
			let child3 = FooChild(fooId: 8, name: "another")
			let txn = try conn.beginTransaction()
			try conn.insert([o, o2])
			try conn.insert([child1, child2, child3])
			try txn.commit()
			let children = try conn.findRelated(KeyedFoo.children, of: o)
			XCTAssertEqual(2, children.count)
			verify(child1, children)
			verify(child2, children)
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	
	func testFindWithOrder() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let o = KeyedFoo(id: 7, name: "my name", description: "desc", senum: .val2, nenum: .val2)
			let o2 = KeyedFoo(id: 8, name: "foo2", description: "desc", senum: .val1, nenum: nil)
			let txn = try conn.beginTransaction()
			try conn.insert([o, o2])
			try txn.commit()
			let pred = Predicate(all: KeyedFoo.self)
			var results = try conn.find(Query(predicate: pred, order: Order(by: \KeyedFoo.name)))
			XCTAssertEqual(2, results.count)
			verify(o2, results[0])
			verify(o, results[1])
			results = try conn.find(Query(predicate: pred, order: Order(by: \KeyedFoo.name, descending: true)))
			XCTAssertEqual(2, results.count)
			verify(o2, results[1])
			verify(o, results[0])
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	func testFindPaged() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let o = KeyedFoo(id: 7, name: "my name", description: "desc", senum: .val2, nenum: .val2)
			let o2 = KeyedFoo(id: 8, name: "foo2", description: "desc", senum: .val1, nenum: nil)
			let txn = try conn.beginTransaction()
			try conn.insert([o, o2])
			try txn.commit()
			try verifyPages(conn, [o,o2], pageSize: Int.max, expectedCalls: 1)
			try verifyPages(conn, [o,o2], pageSize: 2, expectedCalls: 1)
			try verifyPages(conn, [o,o2], pageSize: 1, expectedCalls: 2)
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	private func verifyPages(_ conn: StorageConnection, _ expected: [KeyedFoo], pageSize: Int, expectedCalls: Int) throws {
		var result = [KeyedFoo]()
		var numCalls = 0
		let pred = Predicate(all: KeyedFoo.self)
		let order = Order(by: \KeyedFoo.id)
		try conn.fetch(query: Query(predicate: pred, pageSize: pageSize, order: order)) {
			numCalls += 1
			result = result + $0
			return true
		}
		XCTAssertEqual(expectedCalls, numCalls)
		XCTAssertEqual(expected.count, result.count)
		for (e,r) in zip(expected, result) {
			verify(e, r)
		}
	}
	func testFindJoinedObjects() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let o = KeyedFoo(id: 7, name: "my name", description: "desc", senum: .val2, nenum: .val2)
			let o2 = KeyedFoo(id: 8, name: "foo2", description: "desc", senum: .val1, nenum: nil)
			let child1 = FooChild(fooId: 7, name: "first")
			let child2 = FooChild(fooId: 7, name: "second")
			let child3 = FooChild(fooId: 8, name: "another")
			let txn = try conn.beginTransaction()
			try conn.insert([o, o2])
			try conn.insert([child1, child2, child3])
			try txn.commit()
			let leftPred = Predicate(\KeyedFoo.id %== 7)
				//.property(\.id, .equal(7))
			let rightPred = Predicate(all: FooChild.self)
			let results = try conn.find(JoinedQuery(ParentChild.self, left: leftPred, right: rightPred))
			XCTAssertEqual(2, results.count)
			verify(child1, results.map({$0.child}))
			verify(child2, results.map({$0.child}))
			verify(o, results.map({$0.parent})[0])
			verify(o, results.map({$0.parent})[1])
		} catch {
			XCTFail(error.localizedDescription)
		}
	}

	func testOrderedJoinedObjects() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let o = KeyedFoo(id: 7, name: "my name", description: "desc", senum: .val2, nenum: .val2)
			let o2 = KeyedFoo(id: 8, name: "foo2", description: "desc", senum: .val1, nenum: nil)
			let child1 = FooChild(fooId: 7, name: "first")
			let child3 = FooChild(fooId: 8, name: "another")
			let txn = try conn.beginTransaction()
			try conn.insert([o, o2])
			try conn.insert([child1, child3])
			try txn.commit()
			let leftPred = Predicate(all: KeyedFoo.self)
			let rightPred = Predicate(all: FooChild.self)
			let order = Order(by: \KeyedFoo.name, through: \ParentChild.parent)
			let query = JoinedQuery(ParentChild.self, left: leftPred, right: rightPred, order: order)
			let results = try conn.find(query)
			XCTAssertEqual(2, results.count)
			verify(o2, results.map({$0.parent})[0])
			verify(o, results.map({$0.parent})[1])
		} catch {
			XCTFail(error.localizedDescription)
		}
	}

	func testOptionalComparison() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let o = KeyedFoo(id: 7, name: "my name", description: "desc", senum: .val2, nenum: .val2)
			let o2 = KeyedFoo(id: 8, name: "foo2", description: "desc", senum: .val1, nenum: nil)
			let child1 = OptChild(id: 1, parentId: 7, name: "o1_7")
			let child2 = OptChild(id: 2, parentId: nil, name: "no parent")
			let child3 = OptChild(id: 3, parentId: 7, name: "o2_7")
			let child4 = OptChild(id: 4, parentId: 8, name: "o1_8")
			let txn = try conn.beginTransaction()
			try conn.insert([o, o2])
			try conn.insert([child1, child2, child3, child4])
			try txn.commit()
			
			let pred = \OptChild.parentId %== 7
			let order = Order(by: \OptChild.id)
			let query = Query(predicate: pred, order: order)
			let results = try conn.find(query)
			XCTAssertEqual(2, results.count)
			if results.count >= 2 {
				XCTAssertEqual(1, results[0].id)
				XCTAssertEqual(3, results[1].id)
			}
			let parentsPred = KeyedFoo.optChildren.in(\OptChild.name %== "o1_8")
			let parents = try conn.find(Query(predicate: parentsPred))
			XCTAssertEqual(1, parents.count)
			XCTAssertEqual(8, parents.first?.id)
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	func testOptionalJoinedObjects() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let o = KeyedFoo(id: 7, name: "my name", description: "desc", senum: .val2, nenum: .val2)
			let o2 = KeyedFoo(id: 8, name: "foo2", description: "desc", senum: .val1, nenum: nil)
			let child1 = OptChild(id: 1, parentId: 7, name: "o1_7")
			let child2 = OptChild(id: 2, parentId: nil, name: "no parent")
			let child3 = OptChild(id: 3, parentId: 7, name: "o2_7")
			let child4 = OptChild(id: 4, parentId: 8, name: "o1_8")
			let txn = try conn.beginTransaction()
			try conn.insert([o, o2])
			try conn.insert([child1, child2, child3, child4])
			try txn.commit()
			let leftPred = \KeyedFoo.id %== 7
			let rightPred = Predicate(all: OptChild.self)
			let order = Order(by: \KeyedFoo.name, through: \ParentOptChild.parent)
			let query = JoinedQuery(ParentOptChild.self, left: leftPred, right: rightPred, order: order)
			let results = try conn.find(query)
			XCTAssertEqual(2, results.count)
			if results.count >= 2 {
				verify(o, results.map({$0.parent})[0])
				verify(o, results.map({$0.parent})[1])
			}
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	func testNextId() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			
			let allocator = db.keyAllocator(for: KeyedFoo.self)
			let o = KeyedFoo(id: allocator.next(), name: "my name", description: "desc", senum: .val2, nenum: .val2)
			let o2 = KeyedFoo(id: allocator.next(), name: "foo2", description: "desc", senum: .val1, nenum: nil)
			XCTAssertEqual(1, o.id)
			XCTAssertEqual(2, o2.id)
			try conn.insert([o, o2])
			let results = try conn.find(\KeyedFoo.id %* [o.id, o2.id])
			XCTAssertEqual(2, results.count)
			verify(o, results.first(where: {$0.id == o.id})!)
			verify(o2, results.first(where: {$0.id == o2.id})!)

		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	
	func testNextIdWithExistingRows() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let oFirst = KeyedFoo(id: 1, name: "initial", description: "desc", senum: .val2, nenum: .val2)
			try conn.insert(oFirst)
			let allocator = db.keyAllocator(for: KeyedFoo.self)
			let o = KeyedFoo(id: allocator.next(), name: "my name", description: "desc", senum: .val2, nenum: .val2)
			let o2 = KeyedFoo(id: allocator.next(), name: "foo2", description: "desc", senum: .val1, nenum: nil)
			XCTAssertEqual(2, o.id)
			XCTAssertEqual(3, o2.id)
			try conn.insert([o, o2])
			let results = try conn.find(\KeyedFoo.id %* [o.id, o2.id])
			XCTAssertEqual(2, results.count)
			verify(o, results.first(where: {$0.id == o.id})!)
			verify(o2, results.first(where: {$0.id == o2.id})!)
			
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	private func verify(_ row: Foo, _ values: [Foo]) {
		guard let v = values.first(where: { $0.id == row.id}) else {
			XCTFail("couldn't find row \(row.id)")
			return
		}
		XCTAssertEqual(row.name, v.name)
		XCTAssertEqual(row.description, v.description)
	}
	private func verify(_ row: FooChild, _ values: [FooChild]) {
		guard let v = values.first(where: { $0.name == row.name}) else {
			XCTFail("couldn't find row \(row.name)")
			return
		}
		XCTAssertEqual(row.fooId, v.fooId)
	}
	private func verify(_ row: DoubleKeyed, _ value: DoubleKeyed) {
		XCTAssertEqual(row.leftId, value.leftId)
		XCTAssertEqual(row.rightId, value.rightId)
		XCTAssertEqual(row.name, value.name)
	}
	private func verify(_ row: KeyedFoo, _ value: KeyedFoo) {
		XCTAssertEqual(row.id, value.id)
		XCTAssertEqual(row.name, value.name)
		XCTAssertEqual(row.description, value.description)
		XCTAssertEqual(row.senum, value.senum)
		XCTAssertEqual(row.nenum, value.nenum)
	}
}

fileprivate struct Foo: Codable {
	var id: Int = 0
	var name: String = ""
	var description: String? = nil
}

fileprivate struct KeyedFoo: PrimaryKeyTable, Codable {
	var id: Int = 0
	var name: String = ""
	var description: String? = nil
	var senum: StringEnum = .val1
	var nenum: IntEnum? = nil
	
	static let primaryKey = \KeyedFoo.id
	static let children = toMany(\FooChild.fooId)
	static let optChildren = toMany(\OptChild.parentId)
}

fileprivate struct OptChild: PrimaryKeyTable {
	var id: Int = 0
	var parentId: Int? = nil
	var name: String = ""
	static let primaryKey = \OptChild.id
	static let parent = toOne(KeyedFoo.self, \.parentId)
	static let foreignKeys = [parent]
}
fileprivate struct DoubleKeyed: PrimaryKeyTable2 {
	var leftId: Int = 0
	var rightId: Int = 0
	var name = ""
	static let primaryKey = (\DoubleKeyed.leftId, \DoubleKeyed.rightId)
}

fileprivate struct FooChild: SqlTableRepresentable {
	var fooId: Int = 0
	var name = ""
	static let parent = toOne(KeyedFoo.self, \.fooId)
	static let foreignKeys: [CqlForeignKeyRelation] = [parent]
}

fileprivate struct ParentChild: SqlJoin {
	typealias Property = Int
	
	var parent = KeyedFoo()
	var child = FooChild()
	static let left = \ParentChild.parent
	static let right = \ParentChild.child
//	static let relationship = JoinProperty(left: \KeyedFoo.id, right: \FooChild.fooId)
	static let relationship = KeyedFoo.children.join
}

fileprivate struct ParentOptChild: OptionalSqlJoin {
	typealias Property = Int
	
	var parent = KeyedFoo()
	var child = OptChild()
	static let left = \ParentOptChild.parent
	static let right = \ParentOptChild.child
	static let relationship = KeyedFoo.optChildren.join
}
class MemoryStorageTests: DatabaseTests {
	override func openTestDatabase() throws -> Storage {
		return MemoryStorage()
	}
}
