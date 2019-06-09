//
//  SqlValueTests.swift
//  SqlTests
//
//  Created by Neil Allain on 5/20/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import XCTest
@testable import Cql

class SqlValueTests: XCTestCase {
	
	override func setUp() {
		// Put setup code here. This method is called before the invocation of each test method in the class.
	}
	
	override func tearDown() {
		// Put teardown code here. This method is called after the invocation of each test method in the class.
	}
	
	func testHexEncodedData() {
		let uuid = UUID()
		guard let d = SqlValue.uuid(uuid).dataValue else {
			XCTFail("converting uuid to data")
			return
		}
		let hex = d.hexEncodedString
		XCTAssertTrue(!hex.isEmpty)
		guard let unencoded = Data(hexEncodedString: hex) else {
			XCTFail("could not init data from hex: \(hex)")
			return
		}
		guard let unencodedId = UUID(data: unencoded) else {
			XCTFail("could not init uuid from data")
			return
		}
		XCTAssertEqual(uuid, unencodedId)
	}
	
}
