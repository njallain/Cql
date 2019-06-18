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
				.table(KeyedFoo.self)
			])
	}
	

	func testInsert() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let row = Foo(id: 5, name: "test", description: "cde")
			try conn.insert(row)
			let results = try conn.find(Where.all(Foo.self).property(\.id, .equal(5)))
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
			let results = try conn.find(Where.all(Foo.self))
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
			try conn.update(where: Where.all(Foo.self).property(\.id, .equal(5))) {
				$0.id = updatedRow.id
				$0.name = updatedRow.name
				$0.description = updatedRow.description
			}
			let results = try conn.find(Where.all(Foo.self))
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
			let children = try conn.find(KeyedFoo.children, of: o)
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
			let children = try conn.find(KeyedFoo.children, of: o)
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
			let pred = Where.all(KeyedFoo.self)
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
			let pred = Where.all(KeyedFoo.self)
				.property(\.id, .equal(7))
				.join(children: KeyedFoo.children, Where.all(FooChild.self))
			let results = try conn.find(pred)
			XCTAssertEqual(2, results.count)
			verify(child1, results.map({$0.1}))
			verify(child2, results.map({$0.1}))
			verify(o, results.map({$0.0})[0])
			verify(o, results.map({$0.0})[1])
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
			let pred = Where.all(KeyedFoo.self)
				.join(children: KeyedFoo.children, Where.all(FooChild.self))
			let order = JoinedOrder(Order(by: \KeyedFoo.name), FooChild.self)
			let query = JoinedQuery(predicate: pred, order: order)
			let results = try conn.find(query)
			XCTAssertEqual(2, results.count)
			verify(o2, results.map({$0.0})[0])
			verify(o, results.map({$0.0})[1])
		} catch {
			XCTFail(error.localizedDescription)
		}
	}

	func testNextId() {
		do {
			let db = try openTestDatabase()
			let conn = try db.open()
			let o = KeyedFoo(id: try conn.nextId(KeyedFoo.self), name: "my name", description: "desc", senum: .val2, nenum: .val2)
			let o2 = KeyedFoo(id: try conn.nextId(KeyedFoo.self), name: "foo2", description: "desc", senum: .val1, nenum: nil)
			XCTAssertEqual(1, o.id)
			XCTAssertEqual(2, o2.id)
			try conn.insert([o, o2])
			let results = try conn.find(Where.all(KeyedFoo.self).property(\.id, .any([o.id, o2.id])))
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
			let o = KeyedFoo(id: try conn.nextId(KeyedFoo.self), name: "my name", description: "desc", senum: .val2, nenum: .val2)
			let o2 = KeyedFoo(id: try conn.nextId(KeyedFoo.self), name: "foo2", description: "desc", senum: .val1, nenum: nil)
			XCTAssertEqual(2, o.id)
			XCTAssertEqual(3, o2.id)
			try conn.insert([o, o2])
			let results = try conn.find(Where.all(KeyedFoo.self).property(\.id, .any([o.id, o2.id])))
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
}

fileprivate struct FooChild: SqlTableRepresentable {
	var fooId: Int = 0
	var name = ""
	static let parent = toOne(KeyedFoo.self, \.fooId)
	static let foreignKeys: [ForeignKeyRelation] = [parent]
}

class MemoryStorageTests: DatabaseTests {
	override func openTestDatabase() throws -> Storage {
		return MemoryStorage()
	}
}
