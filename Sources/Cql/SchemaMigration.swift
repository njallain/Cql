//
//  SchemaMigration.swift
//  Sql
//
//  Created by Neil Allain on 5/15/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

/**
Protocol for driver-specific comparison of schemas.
While SqlType supports types like .data, .uuid, etc. those are all translated to 'no type'
in sqlite.  So when reading a schema from sqlite, we need to compare the internal sqlite type.
*/
protocol SchemaDiffer {
	func areEqual(existing existingSqlType: SqlType, expected expectedSqlType: SqlType) -> Bool
	func areEqual(existing existingValue: SqlValue, expected expectedSqlvalue: SqlValue) -> Bool
}

/**
Represents any differences between the schemas of 2 tables
*/
public enum SchemaTableDifference: Equatable, CustomStringConvertible {
	case newColumn(ColumnDefinition)
	case removedColumn(ColumnDefinition)
	case renamedColumn(from: ColumnDefinition, to: ColumnDefinition)
	case newForeignKey(ForeignKey)
	case removedForeignKey(ForeignKey)
	case newIndex(TableIndex)
	case removedIndex(TableIndex)
	case changedPrimaryKey(from: [ColumnDefinition], to: [ColumnDefinition])
	case changedDefault(from: ColumnDefinition, to: ColumnDefinition)
	
	public func canAutoMigrate(from source: TableSchemaProtocol, to target: TableSchemaProtocol) -> Bool {
		switch self {
		case .newColumn:
			return true
		case .removedColumn:
			return true
		case .newForeignKey(let fk):
			guard let fkCol = target.columns.first(where: { $0.name == fk.columnName} ) else {
				fatalError("invalid schema: foreign key not present in columns")
			}
			// can only auto migrate adding a foreign key if it's nullable
			return fkCol.nullable
		case .renamedColumn:
			return true
		case .removedForeignKey:
			return true
		case .newIndex:
			return true
		case .removedIndex:
			return true
		case .changedPrimaryKey:
			return false
		case .changedDefault:
			return true
		}
	}
	private static func areEqual(differ: SchemaDiffer, _ lhs: ColumnDefinition, _ rhs: ColumnDefinition) -> Bool {
		return lhs.name == rhs.name
			&& differ.areEqual(existing: lhs.sqlType, expected: rhs.sqlType)
	}
	private static func contains(_ differ: SchemaDiffer, _ columns: [ColumnDefinition], _ column: ColumnDefinition, ignore: Set<String>) -> Bool {
		return find(differ, columns, column, ignore: ignore) != nil
	}
	private static func find(_ differ: SchemaDiffer, _ columns: [ColumnDefinition], _ column: ColumnDefinition, ignore: Set<String>) -> ColumnDefinition? {
		return columns.first(where: { !ignore.contains($0.name) && areEqual(differ: differ, $0, column)})
	}
	/**
	Returns an array of tuples containing the original column and new column for any columns
	that weren't removed from the old table or added to the new table
	*/
	static func columnMappings(source: TableSchemaProtocol, target: TableSchemaProtocol, differences: [SchemaTableDifference]) -> [(ColumnDefinition, ColumnDefinition)] {
		var mappings = [(ColumnDefinition, ColumnDefinition)]()
		var ignoreCols = Set<String>()
		for diff in differences {
			switch diff {
			case .newColumn(let col):
				ignoreCols.insert(col.name)
			case .removedColumn(let col):
				ignoreCols.insert(col.name)
			case .renamedColumn(let from, let to):
				if from.sqlType == to.sqlType {
					mappings.append((from, to))
					ignoreCols.insert(from.name)
					ignoreCols.insert(to.name)
				}
			default:
				break
			}
		}
		for oldCol in source.columns.filter({ !ignoreCols.contains($0.name) }) {
			if let newCol = target.columns.first(where: {$0 == oldCol}) {
				mappings.append((oldCol, newCol))
			}
		}
		return mappings
	}
	static func compare(
		differ: SchemaDiffer,
		existing existingSchema: TableSchemaProtocol,
		expected expectedSchema: TableSchemaProtocol,
		renamedColumns: [String: String] = [:]) -> [SchemaTableDifference] {
		var diffs = [SchemaTableDifference]()
		var handledOldCols = Set<String>()
		var handledNewCols = Set<String>()
		for (renameFrom, renameTo) in renamedColumns {
			if let fromCol = existingSchema.columns.first(where: { $0.name == renameFrom }),
				let toCol = expectedSchema.columns.first(where: { $0.name == renameTo && fromCol.sqlType == $0.sqlType }) {
				diffs.append(.renamedColumn(from: fromCol, to: toCol))
				handledNewCols.insert(toCol.name)
				handledOldCols.insert(fromCol.name)
			}
		}
		for col in existingSchema.columns.filter({ !handledOldCols.contains($0.name) }) {
			if let match = find(differ, expectedSchema.columns, col, ignore: handledNewCols) {
				if !differ.areEqual(existing: col.defaultValue, expected: match.defaultValue) {
					diffs.append(.changedDefault(from: col, to: match))
				}
			} else {
				diffs.append(.removedColumn(col))
			}
		}
		for col in expectedSchema.columns.filter({ !handledNewCols.contains($0.name) }) {
			if !contains(differ, existingSchema.columns, col, ignore: handledNewCols) {
				diffs.append(.newColumn(col))
			}
		}
		for ndx in existingSchema.indexes {
			if !expectedSchema.indexes.contains(ndx) {
				diffs.append(.removedIndex(ndx))
			}
		}
		for ndx in expectedSchema.indexes {
			if !existingSchema.indexes.contains(ndx) {
				diffs.append(.newIndex(ndx))
			}
		}
		for fk in existingSchema.foreignKeys {
			if !expectedSchema.foreignKeys.contains(fk) {
				diffs.append(.removedForeignKey(fk))
			}
		}
		for fk in expectedSchema.foreignKeys {
			if !existingSchema.foreignKeys.contains(fk) {
				diffs.append(.newForeignKey(fk))
			}
		}
		
		let existingPks = existingSchema.primaryKeyColumns
		let expectedPks = expectedSchema.primaryKeyColumns
		if expectedPks.count != existingPks.count {
			diffs.append(.changedPrimaryKey(from: existingPks, to: expectedPks))
		} else {
			let pksEquivalent = zip(existingPks, expectedPks).reduce(true) { areEq, pkPair in
				return areEq && pkPair.0.name == pkPair.1.name &&
					differ.areEqual(existing: pkPair.0.sqlType, expected: pkPair.1.sqlType)
			}
			if !pksEquivalent {
				diffs.append(.changedPrimaryKey(from: existingPks, to: expectedPks))
			}
		}
		return diffs
	}
	public var description: String {
		switch self {
		case .changedDefault(let o, let n):
			return "changed default - \(o.defaultValue) to \(n.defaultValue)"
		case .changedPrimaryKey(let o, let n):
			return "changed primary key - \(o.map({$0.description}).joined(separator: ", ")) to \(n.map({$0.description}).joined(separator: ", "))"
		case .newColumn(let n):
			return "new column - \(n)"
		case .renamedColumn(let from, let to):
			return "renamed column from \(from.name) to \(to.name)"
		case .newForeignKey(let n):
			return "new foreign key - \(n)"
		case .newIndex(let n):
			return "new index - \(n)"
		case .removedColumn(let o):
			return "removed column - \(o)"
		case .removedIndex(let o):
			return "removed index - \(o)"
		case .removedForeignKey(let o):
			return "removed foreign key - \(o)"
		}
	}
}

public enum SchemaRefactor {
	case renameTable(from: String, to: String)
	case renameColumn(table: String, from: String, to: String)
	
	static func renamed<T: Codable>(class tableType: T.Type, from previousName: String) -> SchemaRefactor {
		return .renameTable(from: previousName, to: String(describing: tableType))
	}
	static func renamed<T: SqlTableRepresentable, P: SqlConvertible>(property: WritableKeyPath<T, P>, from previousName: String) -> SchemaRefactor {
		guard let propertyPath = SqlPropertyPath.path(T(), keyPath: property) else {
			fatalError("could not determine property path for \(property)")
		}
		return .renameColumn(table: String(describing: T.self), from: previousName, to: propertyPath)
	}
	fileprivate static func toDictionaries(_ renames: [SchemaRefactor]) -> ([String: String], [String: [String: String]]) {
		var tableRenames = [String: String]()	// mapping of old table names to new table names
		var columnRenames = [String: [String: String]]()	// mapping of old column names to new column names for each table
		for ren in renames {
			switch ren {
			case .renameTable(let previous, let current):
				tableRenames[previous] = current
			case .renameColumn(let table, let previous, let current):
				if var t = columnRenames[table] {
					t[previous] = current
				} else {
					columnRenames[table] = [previous: current]
				}
			}
		}
		return (tableRenames, columnRenames)
	}
}
public enum SchemaDifference: Equatable, CustomStringConvertible {
	case newTable(TableSchemaProtocol)
	case changedTable(TableSchemaProtocol, TableSchemaProtocol, [SchemaTableDifference])
	case removedTable(TableSchemaProtocol)
	
	var canAutoMigrate: Bool {
		switch self {
		case .newTable:
			return true
		case .removedTable:
			return true
		case .changedTable(let current, let expected, let diffs):
			return diffs.first(where: { !$0.canAutoMigrate(from: current, to: expected) }) == nil
		}
	}
	var fromTable: TableSchemaProtocol? {
		switch self {
		case .newTable:
			return nil
		case .removedTable(let table):
			return table
		case .changedTable(let table, _, _):
			return table
		}
	}
	var toTable: TableSchemaProtocol? {
		switch self {
		case .newTable(let table):
			return table
		case .removedTable:
			return nil
		case .changedTable(_, let table, _):
			return table
		}
	}
	static func canAutoMigrate(_ diffs: [SchemaDifference]) -> Bool {
		return diffs.first(where: { !$0.canAutoMigrate }) == nil
	}
	static func compare(
		differ: SchemaDiffer,
		existing existingTables: [TableSchemaProtocol],
		expected expectedTables: [TableSchemaProtocol],
		refactors: [SchemaRefactor] = []) -> [SchemaDifference] {
		var differences = [SchemaDifference]()	// the final set of differents
		var handledOldTables = Set<String>()	// names of old tables that have been processed
		var handledNewTables = Set<String>() // names of new tables that have been processed
		let (tableRenames, columnRenames) = SchemaRefactor.toDictionaries(refactors)
		
		// function to add the table differences then remove it from the tables that haven't been handeld
		let handleTable: (TableSchemaProtocol, TableSchemaProtocol) -> Void = { oldTable, newTable in
			if handledOldTables.contains(oldTable.name) || handledNewTables.contains(newTable.name) {
				return
			}
			let tableDifferences = SchemaTableDifference.compare(
				differ: differ,
				existing: oldTable,
				expected: newTable,
				renamedColumns: columnRenames[newTable.name] ?? [:])
			if tableDifferences.count > 0 || oldTable.name != newTable.name {
				differences.append(.changedTable(oldTable, newTable, tableDifferences))
			}
			handledOldTables.insert(oldTable.name)
			handledNewTables.insert(newTable.name)
		}
		
		// first handle any tables that have been marked as renamed (and possible changed)
		for oldTable in existingTables {
			if let renamedTo = tableRenames[oldTable.name], let newTable = expectedTables.first(where: {$0.name == renamedTo}) {
				handleTable(oldTable, newTable)
			}
		}
		// next handle any changed table
		for expectedTable in expectedTables {
			if let existingTable = existingTables.first(where: {$0.name == expectedTable.name}) {
				handleTable(existingTable, expectedTable)
			} else if !handledNewTables.contains(expectedTable.name) {
				differences.append(.newTable(expectedTable))
			}
		}
		// finally remove any remaining tables
		let removedTables = existingTables.filter { existing in !handledOldTables.contains(existing.name) }
		for removedTable in removedTables {
			differences.append(.removedTable(removedTable))
		}
		return differences
	}
	
	public var description: String {
		switch self {
		case .newTable(let table):
			return "New table \(table.name)"
		case .changedTable(let oldTable, let table, let diffs):
			let name = oldTable.name == table.name ? table.name : "\(table.name) (previously \(oldTable.name))"
			return "Changed table \(name): \(diffs)"
		case .removedTable(let table):
			return "Removed table \(table.name)"
		}
	}
	public static func==(_ lhs: SchemaDifference, _ rhs: SchemaDifference) -> Bool {
		switch (lhs, rhs) {
		case (.newTable(let lt), .newTable(let rt)):
			return lt.name == rt.name
		case (.changedTable(_, let lt, let ldiffs), .changedTable(_, let rt, let rdiffs)):
			return lt.name == rt.name && ldiffs == rdiffs
		case (.removedTable(let lt), .removedTable(let rt)):
			return lt.name == rt.name
		default:
			return false
		}
	}
}

public struct MigrationContext {
	public let driver: SqlDriver
	private let migrationTableNames: [String: String]
	init(driver: SqlDriver, names: [String: String]) {
		self.driver = driver
		self.migrationTableNames = names
	}
	/**
	If the table has been migrated, this will return the temporary name of the migrated table.
	During a migration, the original table name will still be available using Database.tableName
	*/
	public func tableName<T: Codable>(of table: T.Type) -> String {
		let name = Database.tableName(of: table)
		return migrationTableNames[name] ?? name
	}
}
public typealias SchemaDifferenceMigrator = (MigrationContext, SchemaDifference) throws -> Void
public enum MigrationAction {
	case auto(SchemaDifference)
	case manual(migrator: SchemaDifferenceMigrator, difference: SchemaDifference)
	
	public static func none(_ context: MigrationContext, _ difference: SchemaDifference) {
	}
}

/**
Provides specifics on how to perform a database migration.  The only required method is manualMigration.
*/
public protocol SchemaMigrator {
	/**
	Provide an array of any refactors that should be applied to the migration.
	If a class or property is renamed, this is a good way to indicate that, in order to
	preserve data.
	The default implementation of this will return an empty array.
	*/
	func refactors(from fromSchema: DatabaseSchema, to toSchema: DatabaseSchema) -> [SchemaRefactor]
	/**
	Provide the migration function needed for the given difference.
	This will only be called for a change that cannot be auto migrated
	*/
	func manualMigration(from fromSchema: DatabaseSchema, to toSchema: DatabaseSchema, difference: SchemaDifference) -> SchemaDifferenceMigrator
	
	/**
	Provide an override for an auto migration.
	This will only be called for differences that can be auto migrated.
	The default implementaiton of this will not overrideany automatic migrations.
	*/
	func overrideAutoMigration(from fromSchema: DatabaseSchema, to toSchema: DatabaseSchema, difference: SchemaDifference) -> SchemaDifferenceMigrator?
}
public extension SchemaMigrator {
	func refactors(from fromSchema: DatabaseSchema, to toSchema: DatabaseSchema) -> [SchemaRefactor] {
		return []
	}
	func overrideAutoMigration(from fromSchema: DatabaseSchema, to toSchema: DatabaseSchema, difference: SchemaDifference) -> SchemaDifferenceMigrator? {
		return nil
	}
}

extension SchemaMigrator {
	func migrationActions(differ: SchemaDiffer, from existingSchema: DatabaseSchema, to expectedSchema: DatabaseSchema) -> [MigrationAction] {
		let refactors = self.refactors(from: existingSchema, to: expectedSchema)
		let diffs = SchemaDifference.compare(differ: differ, existing: existingSchema.tables, expected: expectedSchema.tables, refactors: refactors)
		let migrationActions: [MigrationAction] = diffs.map { difference in
			if difference.canAutoMigrate {
				if let migrationOverride = self.overrideAutoMigration(from: existingSchema, to: expectedSchema, difference: difference) {
					return MigrationAction.manual(migrator: migrationOverride, difference: difference)
				}
				return MigrationAction.auto(difference)
			} else {
				let migrator = self.manualMigration(from: existingSchema, to: expectedSchema, difference: difference)
				return MigrationAction.manual(migrator: migrator, difference: difference)
			}
		}
		return migrationActions
	}
}
