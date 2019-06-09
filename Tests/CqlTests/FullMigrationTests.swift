//
//  FullMigrationTests.swift
//  SqlTests
//
//  Created by Neil Allain on 5/29/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import XCTest
@testable import Cql

class FullMigrationTests: XCTestCase {

	private let dbName = "migrationtest"
	
	private var tempDir: URL {
		return FileManager.default.temporaryDirectory
	}
	func cleanup() {
		let db = tempDir.appendingPathComponent(dbName + ".sqlite")
		if (FileManager.default.fileExists(atPath: db.relativePath)) {
			try! FileManager.default.removeItem(at: db)
		}
	}
	
	private func createTestDatabase(tables: [TableSchemaProtocol]) throws -> SqliteDriver {
		let db = try SqliteDriver.open(url: tempDir.appendingPathComponent(dbName + ".sqlite")) as! SqliteDriver
		for schema in tables {
			let createSql = SqliteDriver.sqlForCreate(table: schema)
			try db.execute(sql: createSql, arguments: [])
		}
		return db
	}
	func testMigrate() {
		do {
			let driver = try createTestDatabase(tables: [
				_Project.renamedSchema(to: Project.self),
				_Item.renamedSchema(to: Item.self),
				_Comment.renamedSchema(to: Comment.self),
				OldAttachment.buildSchema()
				])
			try add(driver, projects: [_Project(id: 1, name: "p1", details: "d1"), _Project(id: 2, name: "p2", details: "d2")])
			try add(driver, items: [
				_Item(id: 1, title: "t1", projectId: 1, state: 0, position: 1),
				_Item(id: 2, title: "t2", projectId: 1, state: 1, position: 2),
				_Item(id: 3, title: "t3", projectId: 2, state: 0, position: 1)
				])
			try add(driver, comments: [
				_Comment(id: UUID(), body: "c1", itemId: 1),
				_Comment(id: UUID(), body: "c2", itemId: 2)
				])
			try add(driver, attachments: [
				OldAttachment(id: 1, data: Data([0, 1, 2]), contentType: "application/octet-stream", itemId: 1)
				])
			let newDb = Database(name: dbName, location: tempDir, provider: .sqlite, version: "2", tables: [
				.table(Project.self),
				.table(Item.self),
				.table(ProjectItem.self),
				.table(Comment.self),
				.table(Attachment.self)])
			try newDb.migrate(TestMigrator())
			let schema = try driver.getExistingTables()
			let expectedSchema: [TableSchemaProtocol] = [Project.buildSchema(), Item.buildSchema(), ProjectItem.buildSchema(), Comment.buildSchema(), Attachment.buildSchema(), Database.versionTable]
			let diffs = SchemaDifference.compare(differ: DatabaseProvider.sqlite, existing: schema, expected: expectedSchema)
			XCTAssertEqual(diffs.count, 0, "\(diffs.map({$0.description}).joined(separator: ", "))")
			
			let conn = try newDb.open()
			let projects = try conn.find(Where.any(Project.self))
			XCTAssertEqual(2, projects.count)
			XCTAssertEqual("p1", projects.first(where:{$0.id == 1})?.title)
			XCTAssertEqual("p2", projects.first(where:{$0.id == 2})?.title)
			let items = try conn.find(Where.any(Item.self))
			XCTAssertEqual(3, items.count)
			XCTAssertEqual(ItemState.incomplete, items.first(where: {$0.id == 1})?.state)
			XCTAssertEqual(ItemState.inProgress, items.first(where: {$0.id == 2})?.state)
			
			let projectItems = try conn.find(Where.any(ProjectItem.self))
			XCTAssertEqual(3, projectItems.count)
			XCTAssertEqual(1, projectItems.filter({ $0.projectId == 1 && $0.itemId == 1}).count)
			XCTAssertEqual(1, projectItems.filter({ $0.projectId == 1 && $0.itemId == 2}).count)
			XCTAssertEqual(1, projectItems.filter({ $0.projectId == 2 && $0.itemId == 3}).count)
			
			let comments = try conn.find(Where.any(Comment.self))
			XCTAssertEqual(2, comments.count)
			XCTAssertNotNil(comments.first(where: {$0.body == "c1" && $0.itemId == 1}))
			XCTAssertNotNil(comments.first(where: {$0.body == "c2" && $0.itemId == 2}))
			
			let attachments = try conn.find(Where.any(Attachment.self))
			XCTAssertEqual(1, attachments.count)
			XCTAssertEqual(1, attachments.first?.id)
			XCTAssertEqual(Data([0,1,2]), attachments.first?.data)
		} catch {
			XCTFail(error.localizedDescription)
		}
	}
	
	override func setUp() {
		cleanup()
	}
	
	override func tearDown() {
		cleanup()
	}

	private func add(_ driver: SqliteDriver, projects: [_Project]) throws {
		try driver.beginTransaction()
		for project in projects {
			let sql = "insert into Project (id, name, details) values (\(project.id), '\(project.name)', '\(project.details)')"
			try driver.execute(sql: sql, arguments: [])
		}
		try driver.commitTransaction()
	}
	private func add(_ driver: SqliteDriver, items: [_Item]) throws {
		try driver.beginTransaction()
		let sql = "insert into Item (id, projectId, title, state, position) values ({id}, {projectId}, {title}, {state}, {position})"
		for item in items {
			let args: [SqlArgument] = [
				SqlArgument(name: "id", value: .int(item.id)),
				SqlArgument(name: "projectId", value: .int(item.projectId)),
				SqlArgument(name: "title", value: .text(item.title)),
				SqlArgument(name: "state", value: .int(item.state)),
				SqlArgument(name: "position", value: .real(item.position))
			]
			try driver.execute(sql: sql, arguments: args)
		}
		try driver.commitTransaction()
	}
	private func add(_ driver: SqliteDriver, comments: [_Comment]) throws {
		try driver.beginTransaction()
		let sql = "insert into Comment (id, itemId, body) values ({id}, {itemId}, {body})"
		for comment in comments {
			let args: [SqlArgument] = [
				SqlArgument(name: "id", value: .uuid(comment.id)),
				SqlArgument(name: "itemId", value: .int(comment.itemId)),
				SqlArgument(name: "body", value: .text(comment.body))
			]
			try driver.execute(sql: sql, arguments: args)
		}
		try driver.commitTransaction()
	}
	private func add(_ driver: SqliteDriver, attachments: [OldAttachment]) throws {
		try driver.beginTransaction()
		let sql = "insert into OldAttachment (id, itemId, contentType, data) values ({id}, {itemId}, {contentType}, {data})"
		for attachment in attachments {
			let args: [SqlArgument] = [
				SqlArgument(name: "id", value: .int(attachment.id)),
				SqlArgument(name: "itemId", value: .int(attachment.itemId)),
				SqlArgument(name: "contentType", value: .text(attachment.contentType)),
				SqlArgument(name: "data", value: .data(attachment.data))
			]
			try driver.execute(sql: sql, arguments: args)
		}
		try driver.commitTransaction()
	}
}

fileprivate class TestMigrator: SchemaMigrator {
	func manualMigration(from fromSchema: DatabaseSchema, to toSchema: DatabaseSchema, difference: SchemaDifference) -> SchemaDifferenceMigrator {
		switch difference {
		case .changedTable(_, let to, _):
			if to.name == Database.tableName(of: Comment.self) {
				return TestMigrator.reIdComments
			}
		case .newTable:
			break
		case .removedTable:
			break
		}
		return MigrationAction.none
	}
	func refactors(from fromSchema: DatabaseSchema, to toSchema: DatabaseSchema) -> [SchemaRefactor] {
		return [
			.renameTable(from: Database.tableName(of: OldAttachment.self), to: Database.tableName(of: Attachment.self)),
			.renameColumn(table: Database.tableName(of: Project.self), from: "name", to: "title")
		]
	}
	func overrideAutoMigration(from fromSchema: DatabaseSchema, to toSchema: DatabaseSchema, difference: SchemaDifference) -> SchemaDifferenceMigrator? {
		switch difference {
		case .changedTable:
			break
		case .newTable(let table):
			if table.name == Database.tableName(of: ProjectItem.self) {
				return TestMigrator.createProjectItems
			}
		case .removedTable:
			break
		}
		return nil
	}
	private static func createProjectItems(_ context: MigrationContext, _ difference: SchemaDifference) throws {
		let ins = "insert into \(context.tableName(of: ProjectItem.self)) (projectId, itemId, position) select projectId, id, position from \(Database.tableName(of: Item.self))"
		try context.driver.execute(sql: ins, arguments: [])
	}
	private static func reIdComments(_ context: MigrationContext, _ difference: SchemaDifference) throws {
		let ins = "insert into \(context.tableName(of: Comment.self)) (id, body, itemId) select rowid, body, itemId from \(Database.tableName(of: Comment.self))"
		try context.driver.execute(sql: ins, arguments: [])
	}
}
fileprivate enum ItemState: Int, SqlIntEnum {
	case incomplete = -1
	case inProgress = 1
	case complete = 2
}

// old schema
fileprivate struct _Project: PrimaryKeyTable {
	var id: Int = 0
	var name = ""
	var details = ""
	static let primaryKey = \_Project.id
	static let items = toMany(\_Item.projectId)
}

fileprivate struct _Item: PrimaryKeyTable {
	var id = 0
	var title = ""
	var projectId = 0
	var state = 0
	var position = Double(0)
	static let project = toOne(_Project.self, \.projectId)
	static let primaryKey = \_Item.id
}

fileprivate struct _Comment: PrimaryKeyTable {
	var id = UUID.defaultValue
	var body = ""
	var itemId = 0
	static let item = toOne(_Item.self, \.itemId)
	static let primaryKey = \_Comment.id
}
fileprivate struct OldAttachment: PrimaryKeyTable {
	var id = 0
	var data = Data()
	var contentType = ""
	var itemId = 0
	static let item = toOne(_Item.self, \.itemId)
	static let primaryKey = \OldAttachment.id
}

// new schema
// rename column
fileprivate struct Project: PrimaryKeyTable {
	var id: Int = 0
	var title = ""	// renamed from name
	var details = ""
	static let primaryKey = \Project.id
	static let items = toMany(\ProjectItem.projectId)
}

// new default
fileprivate struct Item: PrimaryKeyTable {
	var id = 0
	var title = ""
	var position = Double(0)
	var state = ItemState.incomplete	// still an int but default should be -1 now
	static let primaryKey = \Item.id
}

// introduce join
fileprivate struct ProjectItem: PrimaryKeyTable2 {
	var projectId = 0
	var itemId = 0
	var position: Double = 0
	static let primaryKey = (\ProjectItem.projectId, \ProjectItem.itemId)
}

// rename table
fileprivate struct Attachment: PrimaryKeyTable{
	var id = 0
	var data = Data()
	var contentType = ""
	var itemId = 0
	static let item = toOne(Item.self, \.itemId)
	static let primaryKey = \Attachment.id
}

// primary key change
fileprivate struct Comment: PrimaryKeyTable {
	var id = 0
	var body = ""
	var itemId = 0
	static let item = toOne(Item.self, \.itemId)
	static let primaryKey = \Comment.id
}
