//
//  InsertBuilder.swift
//  Sql
//
//  Created by Neil Allain on 3/17/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

class ArgumentBuilder: SqlBuilder {
	var values: [SqlArgument] = []
	var intRepresentibles: [String: SqlIntRepresentible.Type] = [:]
	var stringRepresentibles: [String: SqlStringRepresentible.Type] = [:]
	var valuesByName: [String: SqlValue] {
		Dictionary<String, SqlValue>(uniqueKeysWithValues: values.map { ($0.name, $0.value )})
	}
	func add(name: String, value: Int) {
		values.append(SqlArgument(name: name, value: .int(value)))
	}
	func add(name: String, value: Int?) {
		if let value = value {
			values.append(SqlArgument(name: name, value: .int(value)))
		} else {
			values.append(SqlArgument(name: name, value: .null))
		}
	}
	func add(name: String, value: Bool) {
		values.append(SqlArgument(name: name, value: .bool(value)))
	}
	func add(name: String, value: Bool?) {
		if let value = value {
			values.append(SqlArgument(name: name, value: .bool(value)))
		} else {
			values.append(SqlArgument(name: name, value: .null))
		}
	}
	func add(name: String, value: String) {
		values.append(SqlArgument(name: name, value: .text(value)))
	}
	func add(name: String, value: String?) {
		if let value = value {
			values.append(SqlArgument(name: name, value: .text(value)))
		} else {
			values.append(SqlArgument(name: name, value: .null))
		}
	}
	func add(name: String, value: Double) {
		values.append(SqlArgument(name: name, value: .real(value)))
	}
	func add(name: String, value: Double?) {
		if let value = value {
			values.append(SqlArgument(name: name, value: .real(value)))
		} else {
			values.append(SqlArgument(name: name, value: .null))
		}
	}
	func add(name: String, value: Date) {
		values.append(SqlArgument(name: name, value: .date(value)))
	}
	func add(name: String, value: Date?) {
		if let value = value {
			values.append(SqlArgument(name: name, value: .date(value)))
		} else {
			values.append(SqlArgument(name: name, value: .null))
		}
	}
	func add(name: String, value: Data) {
		values.append(SqlArgument(name: name, value: .data(value)))
	}
	func add(name: String, value: Data?) {
		if let value = value {
			values.append(SqlArgument(name: name, value: .data(value)))
		} else {
			values.append(SqlArgument(name: name, value: .null))
		}
	}
	func add(name: String, value: UUID) {
		values.append(SqlArgument(name: name, value: .uuid(value)))
	}
	func add(name: String, value: UUID?) {
		if let value = value {
			values.append(SqlArgument(name: name, value: .uuid(value)))
		} else {
			values.append(SqlArgument(name: name, value: .null))
		}
	}
	func add(name: String, value: SqlIntRepresentible, type: SqlIntRepresentible.Type) {
		values.append(SqlArgument(name: name, value: .int(value.intValue)))
		intRepresentibles[name] = type
	}
	func add(name: String, value: SqlIntRepresentible?, type: SqlIntRepresentible.Type) {
		intRepresentibles[name] = type
		if let value = value {
			values.append(SqlArgument(name: name, value: .int(value.intValue)))
		} else {
			values.append(SqlArgument(name: name, value: .null))
		}
	}
	func add(name: String, value: SqlStringRepresentible, type: SqlStringRepresentible.Type) {
		stringRepresentibles[name] = type
		values.append(SqlArgument(name: name, value: .text(value.stringValue)))
	}
	func add(name: String, value: SqlStringRepresentible?, type: SqlStringRepresentible.Type) {
		stringRepresentibles[name] = type
		if let value = value {
			values.append(SqlArgument(name: name, value: .text(value.stringValue)))
		} else {
			values.append(SqlArgument(name: name, value: .null))
		}

	}

	func addEncoded<T: Encodable>(name: String, value: T) {
		let encoder = JSONEncoder()
		do {
			let v = try encoder.encode(value)
			values.append(SqlArgument(name: name, value: .data(v)))
		} catch {
		}
		
	}
	func addEncoded<T: Encodable>(name: String, value: T?) {
		if let value = value {
			addEncoded(name: name, value: value)
		} else {
			values.append(SqlArgument(name: name, value: .null))
		}
	}
		
	static func build(values: Any) throws -> [SqlArgument] {
		let mirror = Mirror(reflecting: values)
		return try mirror.children.map { child in
			guard let name = child.label else {
				throw DatabaseError("all chlidren in values must be named")
			}
			guard let val = SqlValue.convert(child.value) else {
				throw DatabaseError("value \(name): \(child.value) is not convertable to a sql argument")
			}
			return SqlArgument(name: name, value: val)
		}
	}
}

extension SqlCoder {
	func arguments(for row: T) throws -> [SqlArgument] {
		let builder = ArgumentBuilder()
		try self.encode(row, builder)
		return builder.values
	}
}

