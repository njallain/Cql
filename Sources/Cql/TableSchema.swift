//
//  TableSchema.swift
//  Sql
//
//  Created by Neil Allain on 3/2/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

import SQLite3


enum SchemaError: Error {
	case invalidIndex(String)
}


public protocol TableSchemaProtocol: CustomStringConvertible {
	var name: String {get}
	var columns: [ColumnDefinition] {get}
	var description: String {get}
	var primaryKey: [String] {get}
	var indexes: [TableIndex] {get}
	var foreignKeys: [ForeignKey] {get}
}
extension TableSchemaProtocol {
	var description: String {
		let cols = columns.map({$0.description}).joined(separator: ", ")
		let pks = primaryKey.joined(separator: ", ")
		let ndxes = indexes.map({$0.description}).joined(separator: ", ")
		let fks = foreignKeys.map({$0.description}).joined(separator: ", ")
		return "\(name): columns: \(cols), primary keys: \(pks), indexes: \(ndxes), foreign keys: \(fks)"
	}
	var primaryKeyColumns: [ColumnDefinition] {
		
		let defs = self.primaryKey.map { pk -> ColumnDefinition in
			let def = self.columns.first(where: { d in d.name == pk })
			if (def == nil) {
				fatalError("Invalid table definition: primary key \(pk) not in column definitions")
			}
			return def!
		}
		return defs
	}
}

struct UnknownTableSchema: TableSchemaProtocol {
	var name: String
	var columns: [ColumnDefinition]
	var primaryKey: [String]
	var indexes: [TableIndex]
	var foreignKeys: [ForeignKey]
}

public class TableSchema<T: Codable>: TableSchemaProtocol {
	public var name: String
	public var columns: [ColumnDefinition] = []
	public var description: String {
		let cols = columns.reduce("") {
			$0 + "\n" + $1.description
		}
		return "\(name):\(cols)"
	}
	public var primaryKey: [String] = []
	public var indexes: [TableIndex] = []
	public var foreignKeys: [ForeignKey] = []
	public var sqlCoder: SqlCoder<T>
	let newRow: () -> T
	private var columnsByName: [String: ColumnDefinition] = [:]
	
	init(name: String, newRow: @escaping () -> T) {
		self.name = name
		self.newRow = newRow
		self.sqlCoder = SqlCoder<T>()
	}
	func add(column: ColumnDefinition) {
		self.columns.append(column)
		self.columnsByName[column.name] = column
	}
	func column<V: SqlConvertible>(keyPath: WritableKeyPath<T, V>) -> ColumnDefinition? {
		guard let name = SqlPropertyPath.path(newRow(), keyPath: keyPath) else {
			return nil
		}
		return columnsByName[name]
	}
	func column<V: SqlConvertible>(keyPath: WritableKeyPath<T, V?>) -> ColumnDefinition? {
		guard let name = SqlPropertyPath.path(newRow(), keyPath: keyPath) else {
			return nil
		}
		return columnsByName[name]
	}
}

public struct ColumnDefinition: Equatable, CustomStringConvertible {
	var name: String
	var sqlType: SqlType
	var defaultValue: SqlValue
	var nullable: Bool { return defaultValue == .null }
	
	public var description: String {
		let n = nullable ? " null" : ""
		return "\(name) \(sqlType)\(n)default(\(defaultValue))"
	}
}

public struct TableIndex: Equatable, CustomStringConvertible {
	public var columnNames: [String]
	public var isUnique: Bool
	public init(columnNames: [String], isUnique: Bool) {
		self.columnNames = columnNames
		self.isUnique = isUnique
	}
	public var description: String {
		let u = isUnique ? " unique" : ""
		let cols = columnNames.joined(separator: ", ")
		return "\(cols)\(u)"
	}
}
public struct ForeignKey: Equatable, CustomStringConvertible {
	var columnName: String
	var foreignTable: String
	var foreignColumn: String
	public var description: String {
		return "\(columnName) on \(foreignTable)(\(foreignColumn))"
	}
}

class TableBuilder<T: Codable>: SqlBuilder {
	let schema: TableSchema<T>
	
//	var intAdapters = [String: IntRepresentibleAdapter]()
//	var stringAdapters = [String: StringRepresentibleAdapter]()
	init(schema: TableSchema<T>) {
		self.schema = schema
	}
	func add(name: String, value: Int) {
		addColumn(name: name, type: .int)
	}
	func add(name: String, value: Int?) {
		addColumn(name: name, type: .int, defaultValue: .null)
	}
	func add(name: String, value: Bool) {
		addColumn(name: name, type: .bool)
	}
	func add(name: String, value: Bool?) {
		addColumn(name: name, type: .bool, defaultValue: .null)
	}
	func add(name: String, value: String) {
		addColumn(name: name, type: .text)
	}
	func add(name: String, value: String?) {
		addColumn(name: name, type: .text, defaultValue: .null)
	}
	func add(name: String, value: Double) {
		addColumn(name: name, type: .real)
	}
	func add(name: String, value: Double?) {
		addColumn(name: name, type: .real, defaultValue: .null)
	}
	func add(name: String, value: Date) {
		addColumn(name: name, type: .date)
	}
	func add(name: String, value: Date?) {
		addColumn(name: name, type: .date, defaultValue: .null)
	}
	func add(name: String, value: Data) {
		addColumn(name: name, type: .blob)
	}
	func add(name: String, value: Data?) {
		addColumn(name: name, type: .blob, defaultValue: .null)
	}
	func add(name: String, value: UUID) {
		addColumn(name: name, type: .uuid)
	}
	func add(name: String, value: UUID?) {
		addColumn(name: name, type: .uuid, defaultValue: .null)
	}
	func add<T>(name: String, value: T, type: SqlIntRepresentible.Type) {
		addColumn(name: name, type: .int, defaultValue: .int(type.defaultIntValue))
		//intAdapters[String(describing: T.self)] = type.sqlValueAdapter
	}
	func add<T>(name: String, value: T?, type: SqlIntRepresentible.Type) {
		addColumn(name: name, type: .int, defaultValue: .null)
		//intAdapters[String(describing: T.self)] = type.sqlValueAdapter
	}
	func add<T>(name: String, value: T, type: SqlStringRepresentible.Type) {
		addColumn(name: name, type: .text, defaultValue: .text(type.defaultStringValue))
		//stringAdapters[String(describing: T.self)] = type.sqlValueAdapter
	}
	func add<T>(name: String, value: T?, type: SqlStringRepresentible.Type) {
		addColumn(name: name, type: .text, defaultValue: .null)
	}

	
	func addEncoded<T: Encodable>(name: String, value: T) {
		let d = try! JSONEncoder().encode(value)
		addEncodedColumn(name: name, defaultValue: .data(d))
	}
	func addEncoded<T: Encodable>(name: String, value: T?) {
		addEncodedColumn(name: name, defaultValue: .null)
	}

	func addColumn(name: String, type: SqlType, defaultValue: SqlValue? = nil) {
		let defVal = defaultValue ?? type.defaultValue
		schema.add(column: ColumnDefinition(name: name, sqlType: type, defaultValue: defVal))
	}
	func addEncodedColumn(name: String, defaultValue: SqlValue) {
		schema.add(column: ColumnDefinition(name: name, sqlType: .blob, defaultValue: defaultValue))
	}
	func build() {
		let table = schema.newRow()
		do {
			try schema.sqlCoder.encode(table, self)
		} catch {
			fatalError("could not build schema \(String(describing: T.self))" )
		}
	}
	static func build(schema: TableSchema<T>) -> TableSchema<T>{
		let builder = TableBuilder(schema: schema)
		builder.build()
		return builder.schema
	}
	
	static func build(_ newRow: @escaping () -> T) -> TableSchemaProtocol {
		return build(schema: TableSchema(name: String(describing: T.self), newRow: newRow))
	}

}

extension TableBuilder where T: Initable {
	static func build(table: T) throws -> TableSchemaProtocol {
		return self.build(T.init)
	}
}






//createTable(db: db, table: tableDef)


