//
//  ArgumentBuilderTests.swift
//  SqlTests
//
//  Created by Neil Allain on 3/17/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import XCTest
@testable import Cql

class ArgumentBuilderTests: XCTestCase {
	
	override func setUp() {
		// Put setup code here. This method is called before the invocation of each test method in the class.
	}
	
	override func tearDown() {
		// Put teardown code here. This method is called after the invocation of each test method in the class.
	}
	
	func testBuild() {
		let args = Args(id: 5, name: "test", num: 10, date: Date(timeIntervalSinceReferenceDate: 1000), uuid: UUID(), double: 20.5, bool: true, opt: nil, nenum: .val2, nnenum: .val1, senum: .val1, nsenum: nil)
		do {
			let encoded = try SqlCoder<Args>().arguments(for: args)
			verify(encoded, "id", .int(5))
			verify(encoded, "num", .int(10))
			verify(encoded, "date", .date(args.date))
			verify(encoded, "uuid", .uuid(args.uuid))
			verify(encoded, "name", .text(args.name))
			verify(encoded, "double", .real(args.double))
			verify(encoded, "opt", .null)
			verify(encoded, "nenum", .int(IntEnum.val2.rawValue))
			verify(encoded, "nnenum", .int(IntEnum.val1.rawValue))
			verify(encoded, "senum", .text(StringEnum.val1.rawValue))
			verify(encoded, "nsenum", .null)
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	
	func testBuildTuple() {
		let args = (name: "temp", val: 5)
		do {
			let encoded = try ArgumentBuilder.build(values: args)
			verify(encoded, "name", .text("temp"))
			verify(encoded, "val", .int(5))
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	
	private func verify(_ args: [SqlArgument],_ name: String, _ value: SqlValue) {
		guard let a = args.first(where: { $0.name == name }) else {
			XCTFail("no argument named \(name) found")
			return
		}
		XCTAssertEqual(a.value, value)
	}
	
}

fileprivate struct Args: Codable {
	var id: Int = 0
	var name: String = ""
	var num: Int = 0
	var date: Date = Date()
	var uuid: UUID = UUID()
	var double: Double = 0
	var bool = false
	var opt: String? = nil
	var nenum: IntEnum = .val1
	var nnenum: IntEnum? = nil
	var senum: StringEnum = .val1
	var nsenum: StringEnum? = nil
}
