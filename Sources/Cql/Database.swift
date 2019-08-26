//
//  Database.swift
//  Sql
//
//  Created by Neil Allain on 3/2/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

public enum DatabaseProvider {
	case sqlite
}


public struct DatabaseError: Error {
	public let message: String
	init(_ message: String) {
		self.message = message
	}
}


/**
Provides the correct way to build the schema for a table.
If a class supports the SqlTableRepresentable protocol (for things like primary keys,
foreign keys, indexes), then .table(type) will ensure the schema contains those things
*/
public struct SchemaDefiner {
	let schema: TableSchemaProtocol
	
	/**
	Defines the schema for a plain Codable type.  If the type implements SqlTableRepresentable, use table(type) instead.
	*/
	public static func codable<T: Codable>(_ newRow: @escaping () -> T) -> SchemaDefiner {
		let o = newRow()
		if o as? Initable != nil {
			fatalError("SqlTableRepresentables should be defined with .table")
		}
		let schema = TableBuilder.build(newRow)
		return SchemaDefiner(schema)
	}
	/**
	Defines the schema for a PrimaryKeyTable
	*/
	public static func table<T: PrimaryKeyTable>(_ type: T.Type) -> SchemaDefiner {
		return SchemaDefiner(type.buildSchema())
	}
	/**
	Defines the schema for a PrimaryKeyTable2
	*/
	public static func table<T: PrimaryKeyTable2>(_ type: T.Type) -> SchemaDefiner {
		return SchemaDefiner(type.buildSchema())
	}
	/**
	Defines the schema for a SqlTableRepresentable
  */
	public static func table<T: SqlTableRepresentable>(_ type: T.Type) -> SchemaDefiner {
		return SchemaDefiner(type.buildSchema())
	}
	private init(_ schema: TableSchemaProtocol) {
		self.schema = schema
	}
}

/**
Provides both the locatiion and schema of a database.
*/
public class Database: Storage {
	static let versionTableName = "__SchemaVersion__"
	static let versionTable = UnknownTableSchema(
		name: Database.versionTableName,
		columns: [ColumnDefinition(name: "version", sqlType: .text, defaultValue: .text(""))],
		primaryKey: ["version"],
		indexes: [],
		foreignKeys: [])
	private var tables: [String: TableSchemaProtocol] = [:]
	private let provider: DatabaseProvider
	private var verifiedSchema = false
	public let url: URL
	private let version: String
	private var keyAllocators = [String: Any]()
	/**
	Initializes a new database without any kind of connection to that database.
	- parameters:
		- name: the name of the database (this will be used to name a local file if it's a file based database)
		- location: the folder/location of the database (without the name of the file)
		- provider: the type/driver for the databsae (sqlite, postgresql, etc)
		- version: a version tag for the database.  This has no impact on whether a migration is needed.  It's simply a tag that's saved with the databsae if a migration occurs
		- tables: the complete expected schema of the database.
	*/
	public init(name: String, location: URL, provider: DatabaseProvider, version: String, tables: [SchemaDefiner]) {
		self.provider = provider
		self.version = version
		self.url = location.appendingPathComponent("\(name).\(provider.fileExtension)")
		for t in tables {
			self.tables[t.schema.name] = t.schema
		}
	}

	public static func tableName<T: Codable>(of codable: T) -> String {
		return String(describing: T.self)
	}
	public static func tableName<T: Codable>(of codableClass: T.Type) -> String {
		return String(describing: codableClass)
	}
	public func schema<T: Codable>(for tableType: T.Type) -> TableSchema<T> {
		return self.tables[Database.tableName(of: tableType)] as! TableSchema<T>
	}
	func schemaIfDefined<T: Codable>(for tableType: T.Type) -> TableSchema<T>? {
		guard let t = self.tables[Database.tableName(of: tableType)] else {
			return nil
		}
		return t as? TableSchema<T>
	}
	
	/**
	Applies any migrations that are needed to update the existing schema to the current schema
	- Parameter migrator: provides any extra information about how the migration should be performed
	- Note: The migration does not depend on the version string.
					That is only used as extra information provided to the migrator.
					A migration will occur anytime there is a schema difference.
					In general, migrate should be called before the first open
  */
	public func migrate(_ migrator: SchemaMigrator) throws {
		let conn = try provider.open(url: url)
		let existingSchema = try conn.getExistingSchema()
		let expectedSchema = DatabaseSchema(version: self.version, tables: Array(tables.values), versionTable: Database.versionTable)
		let actions = migrator.migrationActions(differ: provider, from: existingSchema, to: expectedSchema)
		if actions.count > 0 {
			try migrate(driver: conn, from: existingSchema, to: expectedSchema, actions: actions)
		}
	}
	/**
	Opens a connection to the database
	If migrate() is not called
	*/
	public func open() throws -> StorageConnection {
		let driver = try provider.open(url: url)
		if !verifiedSchema {
			let existingSchema = try driver.getExistingSchema()
			let targetSchema = DatabaseSchema(version: version, tables: Array(tables.values), versionTable: Database.versionTable)
			let differences = SchemaDifference.compare(differ: provider, existing: existingSchema.tables, expected: targetSchema.tables)
			var migrationActions = [MigrationAction]()
			// if the migration just consists of new tables, the database will be auto migrated
			// any changed table or removed table must be confirmed
			for difference in differences {
				switch difference {
				case .newTable:
					migrationActions.append(.auto(difference))
				default:
					throw DatabaseError("Migrations needed")
				}
			}
			if !migrationActions.isEmpty {
				try migrate(driver: driver, from: existingSchema, to: targetSchema, actions: migrationActions)
			}
			verifiedSchema = true
		}
		let connection = SqlConnection(database: self, driver: driver)
		return connection
	}
	
	func migrate(driver: SqlDriver, from existingSchema: DatabaseSchema, to targetSchema: DatabaseSchema, actions: [MigrationAction]) throws {
		try driver.migrate(actions: actions)
		if (existingSchema.version != targetSchema.version) {
			if (existingSchema.versionTable == nil) {
				let addVersionTable = MigrationAction.auto(.newTable(Database.versionTable))
				try driver.migrate(actions: [addVersionTable])
			}
			try driver.setSchemaVersion(targetSchema.version)
		}
		verifiedSchema = true
	}
	public func delete() throws {
		if FileManager.default.fileExists(atPath: url.relativePath) {
			try FileManager.default.removeItem(atPath: url.relativePath)
		}
	}
	
	public func keyAllocator<T>(for type: T.Type) -> AnyKeyAllocator<T.Key> where T : PrimaryKeyTable {
		let name = Database.tableName(of: type)
		if let allocator = keyAllocators[name] {
			return allocator as! AnyKeyAllocator<T.Key>
		}
		do {
			let conn = try self.open()
			let allocator = try type.keyAllocator(conn)
			keyAllocators[name] = allocator
			return allocator
		} catch {
			fatalError(error.localizedDescription)
		}
	}
}


public class SqlConnection: StorageConnection {
	private weak var _database: Database?
	private var database: Database { self._database! }
	public var storage: Storage { self._database! }
	private let driver: SqlDriver
	private var insertStatementCache = [String: String]()
	
	fileprivate init(database: Database, driver: SqlDriver) {
		self._database = database
		self.driver = driver
	}
	
	public func beginTransaction() throws -> Transaction {
		try driver.beginTransaction()
		return Transaction(
			commit: { [weak self] in try self?.driver.commitTransaction() },
			rollback: { [weak self] in try self?.driver.rollbackTransaction() }
		)
	}
	
	public func insert<T: Codable>(_ rows: [T]) throws {
		let schema = database.schema(for: T.self)
		for row in rows {
			let args = try schema.sqlCoder.arguments(for: row)
			if let sql = insertStatementCache[schema.name] {
				try driver.execute(sql: sql, arguments: args)
			} else {
				let colSql = args.map({ $0.name }).joined(separator: ", ")
				let valSql = args.map({ "{\($0.name)}" }).joined(separator: ", ")
				let sql = "insert into \(Database.tableName(of: row)) (\(colSql)) values (\(valSql))"
				insertStatementCache[schema.name] = sql
				try driver.execute(sql: sql, arguments: args)
			}
		}
	}
	
	public func update<T: Codable>(where predicate: Predicate<T>, set: (inout T) -> Void) throws {
		let schema = database.schema(for: T.self)
		let sqlBuilder = SqlPredicateCompiler<T>(database: database)
		let clauses = sqlBuilder.compile(predicate)
		let changes = try schema.sqlCoder.changes(for: schema.newRow, change: set)
		let setSql = changes.keys.map({ "\($0) = {set\($0)}" }).joined(separator: ", ")
		var args = changes.map({ (k,v) in SqlArgument(name: "set" + k, value: v)})
		args.append(contentsOf: sqlBuilder.arguments)
		let whereSql = clauses.whereClause.isEmpty ? "" : "where " + clauses.whereClause
		let sql = "update \(schema.name) set \(setSql) \(whereSql)"
		try driver.execute(sql: sql, arguments: args)
	}
	public func update<T: PrimaryKeyTable>(_ rows: [T]) throws {
		try update(keyedRows: rows)
	}
	public func update<T: PrimaryKeyTable2>(_ rows: [T]) throws {
		try update(keyedRows: rows)
	}
	private func update<T: Codable>(keyedRows rows: [T]) throws {
		let schema = database.schema(for: T.self)
		guard schema.primaryKey.count > 0 else {
			fatalError("update can only be called on a keyed table")
		}
		var sql = ""
		for row in rows {
			let values = try schema.sqlCoder.arguments(for: row)
			if (sql.isEmpty) {
				let keyVals = values.filter { schema.primaryKey.contains($0.name) }
				let setVals = values.filter { !schema.primaryKey.contains($0.name) }
				let setSql = (setVals.map { "\($0.name) = {\($0.name)}" }).joined(separator: ", ")
				let predSql = (keyVals.map {"\($0.name) = {\($0.name)}"}).joined(separator: " and ")
				sql = "update \(schema.name) set \(setSql) where \(predSql)"
			}
			try driver.execute(sql: sql, arguments: values)
		}
	}
	public func delete<T: Codable>(_ predicate: Predicate<T>) throws {
		let schema = database.schema(for: T.self)
		let sqlBuilder = SqlPredicateCompiler<T>(database: database)
		let clauses = sqlBuilder.compile(predicate)
		let whereSql = clauses.whereClause.isEmpty ? "" : "where " + clauses.whereClause
		let sql = "delete from \(schema.name) \(whereSql)"
		try driver.execute(sql: sql, arguments: sqlBuilder.arguments)
	}
	public func fetch<T: Codable>(query: Query<T>, results: ([T]) -> Bool) throws {
		let schema = database.schema(for: T.self)
		let sqlBuilder = SqlPredicateCompiler<T>(database: database)
		let sql = sqlBuilder.compile(query)
		let cur = try driver.query(sql: sql.fullSql, bind: sql.selectColumns, arguments:sql.arguments)
		var rows = [T]()
		while let reader = try cur.next() {
			rows.append(try schema.sqlCoder.decode(reader, sqlBuilder.tablePrefix))
			if rows.count == query.pageSize {
				if results(rows) { rows.removeAll() }
				else { return }
			}
		}
		if rows.count > 0 {
			_ = results(rows)
		}
	}
	
	public func fetch<T: AnyJoin>(query: JoinedQuery<T>, results: ([T]) -> Bool) throws {
		let leftSchema = database.schema(for: T.Left.self)
		let rightSchema = database.schema(for: T.Right.self)
		let compiler = SqlPredicateCompiler<T>(database: database)
		let sql = compiler.compile(query)
		let cur = try driver.query(sql: sql.fullSql, bind: sql.selectColumns, arguments:sql.arguments)
		var rows = [T]()
		let leftName = T.leftName(database.schema(for: T.Left.self).newRow())
		let rightName = T.rightName(database.schema(for: T.Right.self).newRow())
		while let reader = try cur.next() {
			let t1 = (try leftSchema.sqlCoder.decode(reader, leftName + "."))
			let t2 = (try rightSchema.sqlCoder.decode(reader, rightName + "."))
			rows.append(T(left: t1, right: t2))
			if rows.count == query.pageSize {
				if results(rows) { rows.removeAll() }
				else { return }
			}
		}
		if rows.count > 0 {
			_ = results(rows)
		}
	}
	
	public func nextId<T>(_ type: T.Type) throws -> Int where T : PrimaryKeyTable, T.Key == Int {
		let schema = database.schema(for: type)
		let sql = "select max(\(schema.primaryKey[0])) as maxId from \(schema.name)"
		let cursor = try driver.query(sql: sql, bind: ["maxId"], arguments: [])
		if let reader = try cursor.next() {
			if let n = try reader.getNullableInt(name: "maxId") {
				return n + 1
			} else {
				return 1
			}
		} else {
			return 1
		}
	}
}

extension DatabaseProvider {
	func open(url: URL) throws -> SqlDriver {
		switch (self) {
		case .sqlite:
			return try SqliteDriver.open(url: url)
		}
	}
	var fileExtension: String {
		switch (self) {
		case .sqlite:
			return "sqlite"
		}
	}
}

struct NoArguments: Encodable {
}

public struct DatabaseSchema {
	public let version: String
	public let tables: [TableSchemaProtocol]
	fileprivate let versionTable: TableSchemaProtocol?
}

public struct DatabaseSchemaDifference {
	public let currentVersion: String
	public let expectedVersion: String
	public let differences: [SchemaDifference]
}
public protocol SqlDriver: AnyObject {
	static func open(url: URL) throws -> SqlDriver
	func beginTransaction() throws
	func commitTransaction() throws
	func rollbackTransaction() throws
	func getExistingTables() throws -> [TableSchemaProtocol]
	func execute(sql: String, arguments: [SqlArgument]) throws
	func query(sql: String, bind: [String], arguments: [SqlArgument]) throws -> SqlCursor
	func migrate(actions: [MigrationAction]) throws
}

fileprivate extension SqlDriver {
	/**
	Gets existing tables of the database.  If the version table
	exists, the version will be returned and the version table will be excluded
	from the list of tables
	*/
	func getExistingSchema() throws -> DatabaseSchema {
		let tables = try getExistingTables()
		if let versionTable = tables.first(where: { $0.name == Database.versionTableName }) {
			let version = try getSchemaVersion()
			return DatabaseSchema(version: version, tables: tables.filter({$0.name != Database.versionTableName }), versionTable: versionTable)
		}
		return DatabaseSchema(version: "", tables: tables, versionTable: nil)
	}

	func getSchemaVersion() throws -> String {
		var version = ""
		do {
			let cursor = try self.query(sql: "select version from \(Database.versionTableName)", bind: ["version"], arguments: [])
			if let reader = try cursor.next() {
				version = try reader.getText(name: "versoin")
			}
		} catch {
			// ignore
		}
		return version
	}
	func setSchemaVersion(_ version: String) throws {
		let sql = "insert into \(Database.versionTableName) (version) values ({version}) on conflict(version) do update set version = {version}"
		try self.execute(sql: sql, arguments: [SqlArgument(name: "version", value: .text(version))])
	}
}
public protocol SqlCursor {
	func next() throws -> SqlReader?
}


