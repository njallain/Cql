//
//  SqiliteTestCase.swift
//  CqlTests
//
//  Created by Neil Allain on 6/16/19.
//

import XCTest
@testable import Cql

class SqiliteTestCase: XCTestCase {
	private let dbName = "databasetest"
	
	private var tempDir: URL {
		return FileManager.default.temporaryDirectory
	}
	func cleanup() {
		let db = tempDir.appendingPathComponent(dbName + ".sqlite")
		if (FileManager.default.fileExists(atPath: db.relativePath)) {
			try! FileManager.default.removeItem(at: db)
		}
	}
	
	func openDatabase(_ schema: [SchemaDefiner]) throws -> Database {
		let db = Database(name: dbName, location: tempDir, provider: .sqlite, version: "1", tables: schema)
		return db
	}
	
	override func setUp() {
		cleanup()
	}
}
