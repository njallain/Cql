//
//  File.swift
//  
//
//  Created by Neil Allain on 6/15/19.
//

import Foundation

import XCTest
@testable import Cql
import Foundation
import Combine

public class FindTests: AsyncStorageTestCase {
	func testGet() {
		reset()
		try! conn.insert(Sample(id: 4, name: "test4"))
		let storage = AsyncStorage { self.mem }
		let expectGet = expectation(description: "get")
		let result = storage.get(Sample.self, 4)
		//Thread.sleep(forTimeInterval: 0.1)
		guard let val = wait(for: result, expect: [expectGet]) else {
			XCTFail("nil result")
			return
		}
		XCTAssertEqual(4, val.id)
	}
	func testGetJoin() {
		reset()
		try! conn.insert(SampleChild(sampleId: 1, childId: 2))
		let storage = AsyncStorage { self.mem }
		let expectGet = expectation(description: "get")
		let result = storage.get(SampleChild.self, 1,2)
		guard let val = wait(for: result, expect: [expectGet]) else {
			XCTFail("nil result")
			return
		}
		XCTAssertNotNil(val)
		XCTAssertEqual(1, val.sampleId)
		XCTAssertEqual(2, val.childId)	}

	func testQuery() {
		logEnabled = true
		defer { logEnabled = false }
		reset()
		try! conn.insert(Sample(id: 4, name: "a"))
		try! conn.insert(Sample(id: 5, name: "b"))
		try! conn.insert(Sample(id: 6, name: "b"))
		let storage = AsyncStorage { self.mem }
		let expectQuery = expectation(description: "query")
		let pred = \Sample.name %== "b"
		let order = Order(by: \Sample.id)
		let query = Query(predicate: pred, pageSize:1, order: order)
		let result = storage.query(where: query)
		//Thread.sleep(forTimeInterval: 0.01)
		guard let rows = wait(for: result, expect: [expectQuery]) else {
			XCTFail("nil result")
			return
		}
		XCTAssertEqual(2, rows.count)
		XCTAssertEqual([5,6], rows.map({$0.id}))
	}

	func testQueryNoResults() {
		reset()
		let storage = AsyncStorage { self.mem }
		let expectQuery = expectation(description: "query")
		let pred = \Sample.name %== "b"
		let query = Query(predicate: pred)
		let result = storage.query(where: query)
		//Thread.sleep(forTimeInterval: 0.01)
		guard let rows = wait(for: result, expect: [expectQuery]) else {
			XCTFail("nil result")
			return
		}
		XCTAssertEqual(0, rows.count)
	}

	
	func testFindJoin() {
		reset()
		try! conn.insert(Sample(id: 4, name: "a"))
		try! conn.insert(Sample(id: 5, name: "b"))
		try! conn.insert(Child2(id: 1, name: "child1"))
		try! conn.insert(Child2(id: 2, name: "child2"))
		try! conn.insert(Child2(id: 3, name: "child3"))
		try! conn.insert(SampleChild(sampleId: 4, childId: 1, position: 1, description: "childa1"))
		try! conn.insert(SampleChild(sampleId: 4, childId: 2, position: 2, description: "childa2"))
		try! conn.insert(SampleChild(sampleId: 5, childId: 1, position: 3, description: "childb1"))
		try! conn.insert(SampleChild(sampleId: 5, childId: 3, position: 3, description: "childb3"))
		let expectQuery = expectation(description: "find join")
		let storage = AsyncStorage { self.mem }
		let query = JoinedQuery(
			SampleJoin.self,
			left: \Sample.id %== 4,
			right: Predicate(all: SampleChild.self),
			pageSize: 1,
			order: Order(by: \SampleChild.childId, through: \SampleJoin.child))
		let result = storage.query(where: query)
		guard let rows = wait(for: result, expect: [expectQuery]) else {
			XCTFail("nil result")
			return
		}
		XCTAssertEqual(2, rows.count)
		XCTAssertEqual([1,2], rows.map({$0.child.childId}))
	}
}

fileprivate struct SampleJoin: SqlJoin {
	typealias Property = Int
	var sample = Sample()
	var child = SampleChild()
	
	static let relationship = Sample.children.join
	static let left = \SampleJoin.sample
	static let right = \SampleJoin.child
}
