//
//  SqlPropertyPathTests.swift
//  SqlTests
//
//  Created by Neil Allain on 4/14/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import XCTest
@testable import Cql
class SqlPropertyPathTests: XCTestCase {

    override func setUp() {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

	func testPathToName() {
		let a = Args()
		let pathName = SqlPropertyPath.path(a, keyPath: \.uuid)
		XCTAssertEqual("uuid", pathName)
		let optPathName = SqlPropertyPath.path(a, keyPath: \.opt)
		XCTAssertEqual("opt", optPathName)
		
		XCTAssertEqual("se", SqlPropertyPath.path(a, keyPath: \.se))
		XCTAssertEqual("nne", SqlPropertyPath.path(a, keyPath: \.nne))
	}

	func testChangedValues() {
		do {
			let changes = try SqlCoder<Args>().changes(for: Args.init) {
				$0.s = "test2"
				$0.n = 5
				$0.opt = nil
				$0.d = Date(timeIntervalSinceReferenceDate: 1)
				$0.se = .val2
				$0.nse = .val1
				$0.ne = .val1
				$0.nne = nil
			}
			XCTAssertEqual(8, changes.count)
			XCTAssertEqual(SqlValue.text("test2"), changes["s"])
			XCTAssertEqual(SqlValue.int(5), changes["n"])
			XCTAssertEqual(SqlValue.null, changes["opt"])
			XCTAssertEqual(SqlValue.date(Date(timeIntervalSinceReferenceDate: 1)), changes["d"])
			XCTAssertEqual(SqlValue.text(StringEnum.val2.rawValue), changes["se"])
			XCTAssertEqual(SqlValue.text(StringEnum.val1.rawValue), changes["nse"])
			XCTAssertEqual(SqlValue.int(IntEnum.val1.rawValue), changes["ne"])
			XCTAssertEqual(SqlValue.null, changes["nne"])
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	
	func testJoinPath() {
		let n = SqlPropertyPath.path(Join(), keyPath: Join.left, value: Parent(), valueKeyPath: Join.relationship.left)
		XCTAssertEqual(n, "myParent")
		let ln = SqlPropertyPath.path(Join(), keyPath: Join.right, value: Child(details: "", parentId: UUID()),
																	valueKeyPath: Join.relationship.right)
		XCTAssertEqual(ln, "myChild")
	}
}

fileprivate struct Args: Codable {
	var uuid = UUID()
	var d = Date(timeIntervalSinceReferenceDate: 0)
	var opt: Int? = nil
	var s = "test"
	var n = 5
	var se = StringEnum.val1
	var nse: StringEnum? = nil
	var ne = IntEnum.val1
	var nne: IntEnum? = nil
}


fileprivate struct Parent: SqlPrimaryKeyTable {
	var id = UUID()
	var name = ""
	
	static let primaryKey = \Parent.id
	static let children = toMany(\Child.parentId)
}

fileprivate struct Child: Codable {
	var details: String
	var parentId: UUID
	
	static let parent = toOne(Parent.self, \Child.parentId)
}

fileprivate struct Join: SqlJoin {
	var myParent: Parent = Parent()
	var myChild: Child = Child(details: "", parentId: UUID())
	static let relationship = JoinProperty(left: \Parent.id, right: \Child.parentId)
	static let left = \Join.myParent
	static let right = \Join.myChild
}
