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
		do {
			
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
}
