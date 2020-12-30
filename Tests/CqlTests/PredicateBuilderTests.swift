//
//  PredicateBuilderTests.swift
//  SqlTests
//
//  Created by Neil Allain on 4/9/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import XCTest
@testable import Cql

class PredicateBuilderTests: XCTestCase {
	
	override func setUp() {
		// Put setup code here. This method is called before the invocation of each test method in the class.
	}
	
	override func tearDown() {
		// Put teardown code here. This method is called after the invocation of each test method in the class.
	}
	
//	func testWhereAny() {
//		var o = CheckMe()
//		let builder = Where.any(CheckMe.self)
//			.property(\.str, .equal("test"))
//			.property(\.n, .equal(5))
//		XCTAssertFalse(builder.modelPredicate.evaluate(o))
//		o.str = "test"
//		XCTAssertTrue(builder.modelPredicate.evaluate(o))
//		o.str = "a"
//		o.n = 5
//		XCTAssertTrue(builder.modelPredicate.evaluate(o))
//	}
//	
//	func testWhereAnyIntEnum() {
//		var o = CheckMe()
//		let builder = Where.any(CheckMe.self)
//			.property(\.ne, .equal(o.ne))
//		XCTAssertTrue(builder.modelPredicate.evaluate(o))
//		o.ne = o.ne.differentValue
//		XCTAssertFalse(builder.modelPredicate.evaluate(o))
//	}
//
//	func testWhereAnyStringEnum() {
//		var o = CheckMe()
//		let builder = Where.any(CheckMe.self)
//			.property(\.se, .equal(o.se))
//		XCTAssertTrue(builder.modelPredicate.evaluate(o))
//		o.se = o.se.differentValue
//		XCTAssertFalse(builder.modelPredicate.evaluate(o))
//	}
//	func testWhereAll() {
//		var o = CheckMe()
//		let builder = Where.all(CheckMe.self)
//			.property(\.str, .equal("test"))
//			.property(\.n, .equal(5))
//		XCTAssertFalse(builder.modelPredicate.evaluate(o))
//		o.str = "test"
//		XCTAssertFalse(builder.modelPredicate.evaluate(o))
//		o.n = 5
//		XCTAssertTrue(builder.modelPredicate.evaluate(o))
//	}
	
	func testValueOperator() {
		let eval = PredicateEvaluator<CheckMe>(storage: MemoryStorage())
		XCTAssertTrue(eval.evaluate(5, PredicateValueOperator.equal(5)))
		XCTAssertFalse(eval.evaluate(4, PredicateValueOperator.equal(5)))
		
		XCTAssertFalse(eval.evaluate(5, PredicateValueOperator.lessThan(5)))
		XCTAssertFalse(eval.evaluate(4, PredicateValueOperator.lessThan(4)))
		XCTAssertTrue(eval.evaluate(4, PredicateValueOperator.lessThan(5)))

		XCTAssertFalse(eval.evaluate(5, PredicateValueOperator.greaterThan(5)))
		XCTAssertFalse(eval.evaluate(4, PredicateValueOperator.greaterThan(5)))
		XCTAssertTrue(eval.evaluate(5, PredicateValueOperator.greaterThan(4)))

		XCTAssertTrue(eval.evaluate(5, PredicateValueOperator.lessThanOrEqual(5)))
		XCTAssertFalse(eval.evaluate(5, PredicateValueOperator.lessThanOrEqual(4)))
		XCTAssertTrue(eval.evaluate(4, PredicateValueOperator.lessThanOrEqual(5)))
		
		XCTAssertTrue(eval.evaluate(5, PredicateValueOperator.greaterThanOrEqual(5)))
		XCTAssertFalse(eval.evaluate(4, PredicateValueOperator.greaterThanOrEqual(5)))
		XCTAssertTrue(eval.evaluate(5, PredicateValueOperator.greaterThanOrEqual(4)))
		
		XCTAssertTrue(eval.evaluate(5, PredicateValueOperator.any([5,4])))
		XCTAssertFalse(eval.evaluate(4, PredicateValueOperator.any([5])))
		
		let id = UUID()
		XCTAssertTrue(eval.evaluate(id, PredicateValueOperator.equal(id)))
	}
}

fileprivate struct CheckMe: PrimaryKeyTable {
	var str: String = ""
	var n: Int = 0
	var f: Double = 0
	var id = UUID()
	var se = StringEnum.val1
	var ne = IntEnum.val1
	
	static let primaryKey = \CheckMe.id
	static let relatedObjs = toMany(\RelatedObject.checkMeId)
}


fileprivate struct RelatedObject: Codable {
	var str: String = ""
	var checkMeId: UUID
	
	static let checkMe = toOne(CheckMe.self, \RelatedObject.checkMeId)
}
