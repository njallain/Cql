//
//  File.swift
//  
//
//  Created by Neil Allain on 6/15/19.
//

import Foundation

import XCTest
@testable import Cql

public class InsertUpdateDeleteTests: AsyncStorageTestCase {
	func testInsertUpdateDelete() {
		reset()
		var updRow = Sample(id: 1, name: "test1")
		let delRow1 = Sample(id: 4, name: "test4")
		let delRow2 = Sample(id: 5, name: "test5")
		try! conn.insert([updRow, delRow1, delRow2])
		let storage = AsyncStorage { self.mem }
		let expectInserted = expectation(description: "inserted")
		let result = storage.transaction({ connection in
			try connection.insert([Sample(id: 2, name: "test2"), Sample(id: 3, name: "test3")])
			updRow.name = "updated"
			try connection.update(updRow)
			try connection.delete(delRow1)
			try connection.delete(delRow2)
		})
		guard let _ = wait(for: result, expect: [expectInserted]) else {
			XCTFail("nil result")
			return
		}
		let rows = try! conn.find(Query(predicate: Predicate.all(Sample.self), order: Order(by: \Sample.id)))
		XCTAssertEqual(3, rows.count)
		XCTAssertEqual([1,2,3], rows.map({$0.id}))
		XCTAssertEqual(["updated", "test2", "test3"], rows.map({$0.name}))
	}
	
	func testChangSets() {
		reset()
		let storage = AsyncStorage { self.mem }
		let expect = expectation(description: "saved")
		let cs = TestChangeSet(storage: storage)
		let parent = cs.sample.new {
			$0.name = "test"
		}
		cs.child.new {
			$0.sampleId = parent.id
			$0.childId = 1
			$0.description = "test child"
		}
		let result = storage.save(cs)
		guard let _ = wait(for: result, expect: [expect]) else {
			XCTFail("saved failed")
			return
		}
		let conn = try! mem.open()
		XCTAssertEqual(1, try! conn.find(all: Sample.self).count)
		XCTAssertEqual(1, try! conn.find(all: SampleChild.self).count)

	}
}

fileprivate class TestChangeSet: Storable {
	let sample: ChangeSet<Sample>
	let child: ChangeSet2<SampleChild>
	init(storage: AsyncStorage) {
		sample = storage.changeSet(for: Sample.self)
		child = storage.changeSet(for: SampleChild.self)
	}

	func save(to connection: StorageConnection) throws {
		let txn = try connection.beginTransaction()
		try connection.save(changeSets: sample, child)
		try txn.commit()
	}
}
