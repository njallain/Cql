//
//  SqliteProvider.swift
//  Sql
//
//  Created by Neil Allain on 3/3/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation
import SQLite3

let destroy = unsafeBitCast(OpaquePointer(bitPattern: -1), to: sqlite3_destructor_type.self)

struct SqliteError: Error {
	let code: Int32
	let message: String
	init(_ code: Int32, _ message: String) {
		self.code = code
		self.message = message
	}
	init(_ code: Int32, _ message: String, _ sql: String) {
		self.init(code, "\(message). SQL: \(sql)")
	}
}

fileprivate struct PreparedStatement {
	let sql: String
	let stmt: OpaquePointer!
	func finalize() {
		sqlite3_finalize(stmt)
	}
	func clearBindings() {
		sqlite3_clear_bindings(stmt)
		sqlite3_reset(stmt)
	}
}
class SqliteDriver: SqlDriver {
	private let db: OpaquePointer
	private init(_ db: OpaquePointer) {
		self.db = db
	}
	deinit {
		for stmt in preparedStatments.values {
			stmt.finalize()
		}
		if #available(iOS 8.2, *) {
			sqlite3_close_v2(self.db)
		} else {
			sqlite3_close(self.db)
		}
	}
	static func open(url: URL) throws -> SqlDriver {
		let fpath = url.relativePath
		var db: OpaquePointer?
		let code = sqlite3_open(fpath, &db)
		if code == SQLITE_OK {
			return SqliteDriver(db!)
		}
		throw DatabaseError("Unable to open database at \(url)")
	}
	
	func getExistingTables() throws -> [TableSchemaProtocol] {
		let tableColumnQuery = """
		SELECT
		m.name AS table_name,
		p.cid AS col_id,
		p.name AS col_name,
		p.type AS col_type,
		p.pk AS col_is_pk,
		p.dflt_value AS col_default_val,
		p.[notnull] AS col_is_not_null
		FROM sqlite_master m
		JOIN pragma_table_info((m.name)) p
		ON m.name <> p.name
		WHERE m.type = 'table'
		ORDER BY table_name, col_id
		"""
		let cursor = try self.query(sql: tableColumnQuery, bind: ["table_name", "col_id", "col_name", "col_type", "col_is_pk", "col_default_val", "col_is_not_null"], arguments: [])
		var tables: [String : UnknownTableSchema] = [:]
		while let reader = try cursor.next() {
			let name = try reader.getText(name: "table_name")
			//let nullable = try !reader.getBool(name: "col_is_not_null")
			var schema = tables[name] ?? UnknownTableSchema(name: name, columns: [], primaryKey: [], indexes: [], foreignKeys: [])
			let defVal = try reader.getText(name: "col_default_val")
			let isPk = try reader.getBool(name: "col_is_pk")
			let sqlType = SqlType.from(sqliteType: try reader.getText(name: "col_type"))
			let col = ColumnDefinition(name: try reader.getText(name: "col_name"),
																 sqlType: sqlType,
																 defaultValue: SqlValue.fromSqlite(literalValue: defVal, type: sqlType)
																 )
			schema.columns.append(col)
			if isPk { schema.primaryKey.append(col.name) }
			tables[name] = schema
		}
		let tableNames = tables.values.map({$0.name})
		for tableName in tableNames {
			guard var table = tables[tableName] else { continue }
			let cursor = try self.query(sql: "select * from pragma_foreign_key_list(\'\(tableName)\')", bind: ["id", "seq", "table", "from", "to", "on_update", "on_delete", "match"], arguments: [])
			while let reader = try cursor.next() {
				let toTable = try reader.getText(name: "table")
				let from = try reader.getText(name: "from")
				let to = try reader.getText(name: "to")
				table.foreignKeys.append(ForeignKey(columnName: from, foreignTable: toTable, foreignColumn: to))
			}
			tables[tableName] = table
		}
		for tableName in tableNames {
			guard var table = tables[tableName] else { continue }
			let cursor = try self.query(sql: "select * from pragma_index_list(\'\(tableName)\') where origin != \'pk\'", bind: ["seq", "name", "unique", "origin", "partial"], arguments: [])
			while let reader = try cursor.next() {
				let ndxName = try reader.getText(name: "name")
				let isUnique = (try reader.getInt(name: "unique")) == 0 ? false : true
				let nameParts = ndxName.split(separator: "_")
				if nameParts.count > 1 {
					let colNames = nameParts.dropFirst().map({String($0)})
					table.indexes.append(TableIndex(columnNames: colNames, isUnique: isUnique))
				}
			}
			tables[tableName] = table
		}
		let t = tables.values.map { $0 as TableSchemaProtocol }
		return Array(t)
	}
	func beginTransaction() throws {
		try execute(sql: "BEGIN TRANSACTION", arguments: [])
	}
	func rollbackTransaction() throws {
		try execute(sql: "ROLLBACK TRANSACTION", arguments: [])
	}
	func commitTransaction() throws {
		try execute(sql: "COMMIT TRANSACTION", arguments: [])
	}
	func execute(sql: String, arguments: [SqlArgument]) throws {
		let stmt = try prepare(sql: sql, arguments: arguments)
		defer {
			cache(statement: stmt)
		}
		let stepRes = sqlite3_step(stmt.stmt)
		guard stepRes == SQLITE_DONE else {
			throw SqliteError(stepRes, "Execute should finish in a single step.", sql)
		}
	}
	
	func query(sql: String, bind: [String], arguments: [SqlArgument]) throws -> SqlCursor {
		let stmt = try prepare(sql: sql, arguments: arguments)
		return SqliteCursor(provider: self, statement: stmt, sql: sql, columns: bind)
	}

	
	private let migrateSuffix = "_migrate"
	func migrate(actions: [MigrationAction]) throws {
		// see https://www.sqlite.org/lang_altertable.html for guidelines on altering sqlite tables
		// 1. turn off foreign keys
		// 2. start a transaction
		// 3. create new/altered tables with a temporary name
		// 4. copy data
		// 5. perform manual migrations
		// 6. drop old table
		// 7. rename new table from temporary name
		// 8. commit the transaction
		// 9. turn on foreign keys
		try execute(sql: "PRAGMA foreign_keys = OFF;", arguments: [])
		defer {
			try! execute(sql: "PRAGMA foreign_keys = ON;", arguments: [])
		}
		try beginTransaction()
		do {
			var renameTables = [TableSchemaProtocol]()
			var dropTables = [String]()
			// create migration tables and copy table
			for action in actions {
				switch action {
				case .auto(let difference):
					switch difference {
					case .newTable(let table):
						try createTable(table: table, tableName: table.name + migrateSuffix)
						renameTables.append(table)
					case .changedTable(let current, let expected, let differences):
						let migrationName = expected.name + migrateSuffix
						try createTable(table: expected, tableName: migrationName)
						renameTables.append(expected)
						let mappings = SchemaTableDifference.columnMappings(source: current, target: expected, differences: differences)
						try copy(columns: mappings, from: current.name, to: migrationName)
						dropTables.append(current.name)
						for diff in differences {
							switch diff {
							case .changedDefault(let oldCol, let newCol):
								try copyNonDefaults(from: (current, oldCol), to: (expected, newCol))
							default:
								break
							}
						}
					case .removedTable(let table):
						dropTables.append(table.name)
					}
				case .manual(_, let difference):
					if let table = difference.toTable {
						try createTable(table: table, tableName: table.name + migrateSuffix)
						renameTables.append(table)
					}
					if let fromTable = difference.fromTable {
						dropTables.append(fromTable.name)
					}
				}
			}
			let tableRename = Dictionary(uniqueKeysWithValues: renameTables.map { ($0.name, $0.name + migrateSuffix) })
			let migrationCtx = MigrationContext(driver: self, names: tableRename)
			for action in actions {
				switch action {
				case .manual(let migrator, let differences):
					try migrator(migrationCtx, differences)
				default:
					break
				}
			}
			for dropTable in dropTables {
				try execute(sql: "DROP TABLE \(dropTable);", arguments: [])
			}
			for renameTable in renameTables {
				try execute(sql: "ALTER TABLE \(renameTable.name)\(migrateSuffix) RENAME TO \(renameTable.name);", arguments: [])
				for ndx in renameTable.indexes {
					try execute(sql: SqliteDriver.sqlForCreate(index: ndx, on: renameTable.name), arguments: [])
				}
			}
		} catch {
			try! rollbackTransaction()
			throw error
		}
		try commitTransaction()
	}
	private func copy(columns: [(ColumnDefinition, ColumnDefinition)], from sourceTable: String, to targetTable: String) throws {
		let targetCols = columns.map({ $0.1.name }).joined(separator: ", ")
		let sourceCols = columns.map({ $0.0.name }).joined(separator: ", ")
		try execute(sql: "insert into \(targetTable) (\(targetCols)) select \(sourceCols) from \(sourceTable)", arguments: [])
	}
	private func copyNonDefaults(from: (TableSchemaProtocol, ColumnDefinition), to: (TableSchemaProtocol, ColumnDefinition)) throws {
		let (fromTable, fromCol) = from
		let (toTable, toCol) = to
		let toTableName = toTable.name + migrateSuffix
		let idJoin = zip(fromTable.primaryKey, toTable.primaryKey).map({ "\($0.0) = \(toTableName).\($0.1)"}).joined(separator: " and ")
		let subSelect = "(select \(fromCol.name) from \(fromTable.name) where \(idJoin) and \(fromCol.name) != \(fromCol.defaultValue.sqliteLiteralValue))"
		let sql = "update \(toTableName) set \(toCol.name) = \(subSelect) where exists \(subSelect)"
		try execute(sql: sql, arguments: [])
	}
	private func createTable(table: TableSchemaProtocol, tableName: String) throws {
		let sql = SqliteDriver.sqlForCreate(table: table, tableName: tableName)
		try self.execute(sql: sql, arguments: [])
	}
	static func sqlForCreate(table: TableSchemaProtocol) -> String {
		return sqlForCreate(table: table, tableName: table.name)
	}
	private static func sqlForCreate(table: TableSchemaProtocol, tableName: String) -> String {
		let cols = table.columns.map { sqlForCreate(column: $0) }
		let fks = table.foreignKeys.map { sqlForCreate(foreignKey: $0 )}
		let tableCols = (cols + fks).joined(separator: ", ")
		let withoutRowId = !table.primaryKey.isEmpty ? " WITHOUT ROWID" : ""
		let pks = table.primaryKey.count > 0 ? ", PRIMARY KEY (\(table.primaryKey.joined(separator: ", ")))" : ""
		return "CREATE TABLE \(tableName)(\(tableCols)\(pks))\(withoutRowId);"
//		return "CREATE TABLE \(tableName)(\(tableCols)\(pks));"
	}
	
	private static func sqlForCreate(foreignKey: ForeignKey) -> String {
		return "FOREIGN KEY(\(foreignKey.columnName)) REFERENCES \(foreignKey.foreignTable)(\(foreignKey.foreignColumn))"
	}
	private static func sqlForCreate(column: ColumnDefinition) -> String {
		let n = column.nullable ? "" : " NOT NULL"
		let dflt = column.defaultValue.sqliteLiteralValue
		let sql = "\(column.name) \(column.sqlType.sqliteType) default \(dflt)\(n)"
		return sql
	}
	
	static func sqlForCreate(index: TableIndex, on table: String) -> String {
		let indexColNames = index.columnNames.joined(separator: "_")
		let indexName = "\(table)_\(indexColNames)"
		let indexCols = index.columnNames.joined(separator: ", ")
		let u = index.isUnique ? "UNIQUE " : ""
		return "CREATE \(u)INDEX \(indexName) on \(table)(\(indexCols));"
	}
	private var preparedStatments = [String: PreparedStatement]()
	private func preparedStatement(sql: String, arguments: [SqlArgument]) throws -> PreparedStatement {
		if let stmt = preparedStatments[sql] {
			return stmt
		}
		var paramSql = sql
		for arg in arguments {
			paramSql = paramSql.replacingOccurrences(of: "{\(arg.name)}", with: ":\(arg.name)")
		}
		var dbStmt: OpaquePointer?
		let res = sqlite3_prepare_v2(db, paramSql, -1, &dbStmt, nil)
		guard res == SQLITE_OK else {
			let sqlErr = sqlite3_errmsg(db)
			let errMsg = sqlErr != nil ? String(cString: sqlite3_errmsg(db)) : "Unknown error preparing statement"
			throw SqliteError(res, errMsg, sql)
		}
		return PreparedStatement(sql: sql, stmt: dbStmt)
	}
	fileprivate func cache(statement: PreparedStatement) {
//		statement.finalize()
		statement.clearBindings()
		preparedStatments[statement.sql] = statement
	}
	private func prepare(sql: String, arguments: [SqlArgument]) throws -> PreparedStatement {
		let stmt = try preparedStatement(sql: sql, arguments: arguments)
		for arg in arguments {
			let ndx = sqlite3_bind_parameter_index(stmt.stmt, ":" + arg.name)
			var code = SQLITE_OK
			if ndx > 0 {
				switch arg.value {
				case .null:
					code = sqlite3_bind_null(stmt.stmt, ndx)
				case .bool(let v):
					code = sqlite3_bind_int(stmt.stmt, ndx, v ? 1 : 0)
				case .int(let v):
					code = sqlite3_bind_int64(stmt.stmt, ndx, sqlite3_int64(v))
				case .real(let v):
					code = sqlite3_bind_double(stmt.stmt, ndx, v)
				case .text(let v):
					code = sqlite3_bind_text(stmt.stmt, ndx, v, -1, destroy)
				case .data(let v):
					code = v.withUnsafeBytes { (raw: UnsafeRawBufferPointer) in
						let ptr = raw.bindMemory(to: UInt8.self)
						if let bytes = ptr.baseAddress {
							return sqlite3_bind_blob(stmt.stmt, ndx, bytes, Int32(ptr.count), destroy)
						}
						return 0
					}
				case .uuid(let v):
					var vv = v
					code = withUnsafePointer(to: &vv) { bytes in
						sqlite3_bind_blob(stmt.stmt, ndx, bytes, Int32(MemoryLayout.size(ofValue: v)), destroy)
					}
				case .date(let v):
					code = sqlite3_bind_double(stmt.stmt, ndx, v.timeIntervalSinceReferenceDate)
				}
				try throwIfError(code: code, message: "error binding argument \(arg.name)")
			}
		}
		return stmt
	}
	
	func throwIfError(code: Int32, message: String) throws {
		guard code == SQLITE_OK else {
			let sqlErr = sqlite3_errmsg(db)
			let errMsg = sqlErr != nil ? String(cString: sqlite3_errmsg(db)) : "Unknown error preparing statement"
			throw SqliteError(code, errMsg, message)
		}

	}
}

fileprivate class SqliteCursor: SqlCursor, SqlReader {
	let statement: PreparedStatement
	weak var provider: SqliteDriver?
	let sql: String
	private var columnIndexes: [String: Int] = [:]
	init(provider: SqliteDriver, statement: PreparedStatement, sql: String, columns: [String]) {
		for (i,s) in columns.enumerated() { columnIndexes[s] = i }
		self.provider = provider
		self.statement = statement
		self.sql = sql
	}
	deinit {
		provider?.cache(statement: statement)
	}
	func next() throws -> SqlReader? {
		let code = sqlite3_step(statement.stmt)
		if code == SQLITE_ROW {
			return self
		} else if code == SQLITE_DONE {
			return nil
		}
		try provider?.throwIfError(code: code, message: sql)
		return nil
	}
	
	func getText(index: Int) throws -> String {
		return String(cString: sqlite3_column_text(statement.stmt, Int32(index)))
	}
	func getNullableInt(name: String) throws -> Int? {
		guard let n = index(name: name) else {
			return nil
		}
		return Int(sqlite3_column_int(statement.stmt, n))
	}
	
	func getNullableReal(name: String) throws -> Double? {
		guard let n = index(name: name) else {
			return nil
		}
		return Double(sqlite3_column_double(statement.stmt, n))
	}
	
	func getNullableText(name: String) throws -> String? {
		guard let n = index(name: name) else {
			return nil
		}
		return String(cString: sqlite3_column_text(statement.stmt, n))
	}
	
	func getNullableBool(name: String) throws -> Bool? {
		guard let n = index(name: name) else {
			return nil
		}
		return sqlite3_column_int(statement.stmt, n) != 0 ? true : false
	}
	
	func getNullableDate(name: String) throws -> Date? {
		guard let n = index(name: name) else {
			return nil
		}
		let ts = TimeInterval(sqlite3_column_double(statement.stmt, n))
		return Date(timeIntervalSinceReferenceDate: ts)
	}
	
	func getNullableUuid(name: String) throws -> UUID? {
		guard let n = index(name: name) else {
			return nil
		}
		let count = sqlite3_column_bytes(statement.stmt, n)
		if count != 16 {
			return nil
		}
		guard let bytes = sqlite3_column_blob(statement.stmt, n) else {
			return nil
		}
		let data = Data(bytes: bytes, count: Int(count))
		return UUID(uuid: uuid_t(
			data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
			data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15]))
		
	}
	
	func getNullableBlob(name: String) throws -> Data? {
		guard let n = index(name: name) else {
			return nil
		}
		let count = sqlite3_column_bytes(statement.stmt, n)
		if (count == 0) {
			return nil
		}
		guard let bytes = sqlite3_column_blob(statement.stmt, n) else {
			return nil
		}
		return Data(bytes: bytes, count: Int(count))
	}
	
	func contains(name: String) throws -> Bool {
		return index(name: name) != nil
	}
	
	private func index(name: String) -> Int32? {
		guard let n = columnIndexes[name] else {
			return nil
		}
		let ndx = Int32(n)
		let colType = sqlite3_column_type(statement.stmt, ndx)
		guard colType != SQLITE_NULL else {
			return nil
		}
		return ndx
	}
}

extension SqlType {
	var sqliteType: String {
		switch self {
		case .blob, .uuid:
			return ""
		case .int, .bool:
			return "NUM"
		case .real, .date:
			return "REAL"
		case .text:
			return "TEXT"
		}
	}
	
	static func from(sqliteType: String) -> SqlType {
		switch sqliteType {
		case SqlType.int.sqliteType:
			return .int
		case SqlType.text.sqliteType:
			return .text
		case SqlType.real.sqliteType:
			return .real
		default:
			return .blob
		}
	}
}


fileprivate extension SqlValue {
	var sqliteLiteralValue: String {
		switch self {
		case .null:
			return "null"
		case .int(let v):
			return v.description
		case .real(let v):
			return v.description
		case .text(let v):
			return #"'\#(v)'"#
		case .bool(let v):
			return v ? "1" : "0"
		case .uuid(let v):
			let uid = v.uuid
			let bytes = [uid.0, uid.1, uid.2, uid.3, uid.4, uid.5, uid.6, uid.7, uid.8, uid.9, uid.10, uid.11, uid.12, uid.13, uid.14, uid.15]
			return #"x'\#(Data(bytes).hexEncodedString)'"#
		case .data(let v):
			return #"x'\#(v.hexEncodedString)'"#
		case .date(let v):
			return String(format: "%.5f", v.timeIntervalSinceReferenceDate)
		}
	}
	func isEquivalent(toSqliteValue other: SqlValue) -> Bool {
		switch (self, other) {
		case (.null, .null):
			return true
		case (.int(let v1), .int(let v2)):
			return v1 == v2
		case (.real(let v1), .real(let v2)):
			return v1 == v2
		case (.text(let v1), .text(let v2)):
			return v1 == v2
		case (.bool(let v1), .bool(let v2)):
			return v1 == v2
		case (.bool(let v1), .int(let v2)):
			return v1 == (v2 != 0)
		case (.uuid(let v1), .uuid(let v2)):
			return v1 == v2
		case (.uuid(let v1), .data(let v2)):
			return v1 == UUID(data: v2)
		case (.data(let v1), .data(let v2)):
			return v1 == v2
		case (.date(let v1), .date(let v2)):
			return v1 == v2
		case (.date(let v1), .real(let v2)):
			return v1 == Date(timeIntervalSinceReferenceDate: TimeInterval(v2))
		default:
			return false
		}
	}
	private static func dataFromLiteral(_ literal: String) -> Data {
		if literal.count > 3 {
			let bytes = String(literal.dropFirst("x\'".count).dropLast(1))
			if let d = Data(hexEncodedString: bytes) {
				return d
			}
		}
		return Data()
	}
	static func fromSqlite(literalValue: String, type: SqlType) -> SqlValue {
		if literalValue == SqlValue.null.sqliteLiteralValue { return .null }
		switch type {
		case .int:
			guard let v = Int(literalValue) else {
				return type.defaultValue
			}
			return .int(v)
		case .real:
			guard let v = Double(literalValue) else {
				return type.defaultValue
			}
			return .real(v)
		case .bool:
			guard let v = Int(literalValue) else {
				return type.defaultValue
			}
			return .bool(v != 0)
		case .text:
			var t = literalValue
			if t.starts(with: "\'") { t = String(t.dropFirst())}
			if t.hasSuffix("\'") { t = String(t.dropLast())}
			return .text(t)
		case .blob:
			return .data(dataFromLiteral(literalValue))
		case .uuid:
			let d = dataFromLiteral(literalValue)
			guard let id = UUID(data: d) else {
				return type.defaultValue
			}
			return .uuid(id)
		case .date:
			guard let v = Double(literalValue) else {
				return type.defaultValue
			}
			return .date(Date(timeIntervalSinceReferenceDate: TimeInterval(v)))
		}
	}
}

extension DatabaseProvider: SchemaDiffer {
	func areEqual(existing existingSqlType: SqlType, expected expectedSqlType: SqlType) -> Bool {
		switch self {
		case .sqlite:
			return existingSqlType.sqliteType == expectedSqlType.sqliteType
		}
	}
	func areEqual(existing existingValue: SqlValue, expected expectedSqlvalue: SqlValue) -> Bool {
		return expectedSqlvalue.isEquivalent(toSqliteValue: existingValue)
	}
}
