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
}
