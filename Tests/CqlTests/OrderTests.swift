//
//  OrderTests.swift
//  
//
//  Created by Neil Allain on 6/15/19.
//

import Foundation
import XCTest
@testable import Cql

class OrderTests: SqiliteTestCase {
	func testLessThan() {
		let a = AllTable(n: 5, s: "abc")
		let b = AllTable(n: 4, s: "def")
		let c = AllTable(n: 4, s: "abc")
		let c2 = AllTable(n: 4, s: "abc")
		
		var order = Order.by(\AllTable.n)
		XCTAssertTrue(order.lessThan(b, a))
		XCTAssertFalse(order.lessThan(a, b))
		XCTAssertFalse(order.lessThan(b, c))
		XCTAssertFalse(order.lessThan(c, b))
		XCTAssertTrue(order.equalTo(b,c))
		
		order = order.then(by: \AllTable.s)
		XCTAssertFalse(order.lessThan(b, c))
		XCTAssertTrue(order.lessThan(c, b))
		XCTAssertFalse(order.lessThan(c, c2))
		XCTAssertFalse(order.lessThan(c2, c))
		XCTAssertTrue(order.equalTo(c, c2))
	}
	
	func testLessThanDescending() {
		let a = AllTable(n: 5, s: "abc")
		let b = AllTable(n: 4, s: "def")
		
		let order = Order.by(\AllTable.n, descending: true)
		XCTAssertFalse(order.lessThan(b, a))
		XCTAssertTrue(order.lessThan(a, b))
	}
	
	func testSql() {
		do {
			let database = try openDatabase([.table(AllTable.self)])
			let compiler = SqlPredicateCompiler<AllTable>(database: database)
			let order = Order.by(\AllTable.n).then(by: \AllTable.dt, descending: true)
			let sql = order.sql(compiler: compiler)
			XCTAssertEqual("order by t0.n asc, t0.dt desc", sql)
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
}
