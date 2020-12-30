//
//  AsyncStorageTests.swift
//  
//
//  Created by Neil Allain on 6/14/19.
//
import XCTest
@testable import Cql
import Foundation
import Combine


public class AsyncStorageTestCase: XCTestCase {
	var mem: MemoryStorage { Self._mem! }
	static private var _mem: MemoryStorage? = nil
	var conn: StorageConnection { Self._conn! }
	static private var _conn: StorageConnection? = nil

	override public class func setUp() {
		_mem = MemoryStorage()
		_conn = try! _mem?.open()
	}
	public func reset() {
		AsyncStorageTestCase.tearDown()
		AsyncStorageTestCase.setUp()
	}
	override public class func tearDown() {
		_conn = nil
		_mem = nil
	}

	func wait<T>(for future: AnyPublisher<T, Error>, expect: [XCTestExpectation], enforceOrder: Bool = false) -> T? {
		var finalValue: T? = nil
		let publisher = future.print()
		let _ = publisher.sink(receiveCompletion: { completion in
			switch completion {
			case .finished:
				expect.last?.fulfill()
			case .failure(let error):
				XCTFail(error.localizedDescription)
			}
		}, receiveValue: { value in
			finalValue = value
		})
		wait(for: expect, timeout: 2.5)
		return finalValue
	}

	func assertComplete(_ completion: Subscribers.Completion<Error>, _ expectation: XCTestExpectation) {
		switch completion {
		case .finished:
			break
		case .failure(let error):
			XCTFail(error.localizedDescription)
		}
		expectation.fulfill()
	}
//	override public class func setUp() {
//		_mem = MemoryStorage()
//		_conn = try! _mem?.open()
//	}
//	override public class func tearDown() {
//		_conn = nil
//		_mem = nil
//	}
}
public class AsyncStorageTests: AsyncStorageTestCase {
	
	func testInitializationSuccess() {
		let expectComplete = expectation(description: "complete")
		let storage = AsyncStorage {
			defer { expectComplete.fulfill() }
			Thread.sleep(forTimeInterval: 0.01)
			return MemoryStorage()
		}
		assertReady(storage, false)
		wait(for: [expectComplete], timeout: 0.1)
		assertReady(storage, true)
	}

	func testInitializationFailure() {
		let expectComplete = expectation(description: "complete")
		let storage = AsyncStorage {
			defer { expectComplete.fulfill() }
			throw InitError()
		}
		wait(for: [expectComplete], timeout: 0.1)
		switch storage.ready {
		case .success:
			XCTFail("expected error")
		case .failure(let e):
			print(e.localizedDescription)
			break
			//XCTAssertEqual("InitError", e.localizedDescription)
		}
	}
	
	func testFindDuringInit() {
		let expectInit = expectation(description: "init")
		let expectComplete = expectation(description: "complete")
		let storage = AsyncStorage {
			defer { expectInit.fulfill() }
			Thread.sleep(forTimeInterval: 0.01)
			let mem = MemoryStorage()
			let conn = try! mem.open()
			try! conn.insert(Sample(id: 1, name: "test"))
			return mem
		}
		let query = Query(predicate: Predicate.all(Sample.self))
		let result = storage.query(where: query)
		guard let rows = wait(for: result, expect: [expectInit, expectComplete], enforceOrder: true) else {
			XCTFail("nil result")
			return
		}
		XCTAssertEqual(1, rows.count)
		XCTAssertEqual(1, rows.first?.id)
	}
	
	private func assertReady(_ storage: AsyncStorage, _ ready: Bool) {
		switch storage.ready {
		case.success(let r):
			XCTAssertEqual(ready, r)
		case.failure:
			XCTFail()
		}
	}
	
	static var allTests = [
		("testInitializationSuccess", testInitializationSuccess),
	]
}

extension Result {
	var value: Success? {
		switch self {
		case .success(let s):
			return s
		case .failure:
			return nil
		}
	}
	var isSuccess: Bool {
		switch self {
		case .success:
			return true
		case .failure:
			return false
		}
	}
}
struct Sample: PrimaryKeyTable {
	var id: Int = 0
	var name: String = ""
	static let primaryKey = \Sample.id
	static let children = toMany(\SampleChild.sampleId)
}
struct SampleChild: PrimaryKeyTable2 {
	var sampleId: Int = 0
	var childId: Int = 0
	var position: Double = 0
	var description: String = ""
	static let sample = toOne(Sample.self, \SampleChild.sampleId)
	static let child = toOne(Child2.self, \SampleChild.childId)
	static let primaryKey = (\SampleChild.sampleId, \SampleChild.childId)
}

struct Child2: PrimaryKeyTable {
	var id: Int = 0
	var name: String = ""
	static let primaryKey = \Child2.id
	static let samples = toMany(\SampleChild.childId)
}

fileprivate struct InitError: Error {
	var localizedDescription: String { return "InitError" }
}


