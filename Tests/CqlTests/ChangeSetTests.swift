//
//  File.swift
//  
//
//  Created by Neil Allain on 8/25/19.
//

import Foundation
import XCTest
@testable import Cql

class ChangeSetTests: XCTestCase {
	var storage: Storage = MemoryStorage()
	
	override func setUp() {
		storage = MemoryStorage()
	}
	
	func testNew() {
		let changeSet = storage.changeSet(for: AllTable.self)
		var row = changeSet.new {
			$0.s = "test"
		}
		row.n = 5
		changeSet.updated(row)
		XCTAssertEqual(1, changeSet.newRows.count)
		XCTAssertEqual(0, changeSet.updatedRows.count)
	}
	
	func testSaveOrder() {
		do {
			var changeSet = TestChangeSet(storage: storage)
			var a1 = changeSet.all.new {
				$0.s = "all1"
			}
			let a2 = changeSet.all.new {
				$0.s = "all2"
			}
			let c1 = changeSet.child.new {
				$0.lastName = "child1"
			}
			let c2 = changeSet.child.new {
				$0.lastName = "child2"
			}
			changeSet.join.new {
				$0.allId = a1.id
				$0.childId = c1.id
				$0.description = "a1c1"
			}
			changeSet.join.new {
				$0.allId = a1.id
				$0.childId = c2.id
				$0.description = "a1c2"
			}
			let j_a2_c1 = changeSet.join.new {
				$0.allId = a2.id
				$0.childId = c1.id
				$0.description = "a2c1"
			}
			let conn = try storage.open()
			try changeSet.save(to: conn)
			
			var alls = try conn.find(Predicate.all(AllTable.self))
			var joins = try conn.find(Predicate.all(JoinTable.self))
			var children = try conn.find(Predicate.all(ChildTable.self))
			XCTAssertEqual(2, alls.count)
			XCTAssertEqual(3, joins.count)
			XCTAssertEqual(2, children.count)
			changeSet = TestChangeSet(storage: self.storage)
			let a3 = changeSet.all.new {
				$0.s = "a3"
			}
			a1.s = "a1_update"
			changeSet.all.updated(a1)
			changeSet.all.deleted(a1)
			changeSet.join.deleted(j_a2_c1)
			
			try changeSet.save(to: conn)
			alls = try conn.find(Predicate.all(AllTable.self))
			joins = try conn.find(Predicate.all(JoinTable.self))
			children = try conn.find(Predicate.all(ChildTable.self))
			XCTAssertEqual(2, alls.count)
			XCTAssertEqual(2, joins.count)
			XCTAssertEqual(2, children.count)
			let fa3 = alls.first { $0.id == a3.id }
			XCTAssertEqual(a3.s, fa3?.s)
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
}

fileprivate class TestChangeSet {
	let all: ChangeSet<AllTable>
	let join: ChangeSet2<JoinTable>
	let child: ChangeSet<ChildTable>
	
	init(storage: Storage) {
		all = storage.changeSet(for: AllTable.self)
		join = storage.changeSet(for: JoinTable.self)
		child = storage.changeSet(for: ChildTable.self)
	}
	
	func save(to connection: StorageConnection) throws {
		let txn = try connection.beginTransaction()
		try connection.save(changeSets: all, child, join)
		try txn.commit()
	}
}
