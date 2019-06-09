//
//  SqlEncoder.swift
//  Sql
//
//  Created by Neil Allain on 3/3/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

enum SqlType: String, Equatable {
	case int
	case bool
	case text
	case real
	case date
	case uuid
	case blob
	
	var defaultValue: SqlValue {
		switch self {
		case .int:
			return Int.defaultSqlValue
		case .bool:
			return Bool.defaultSqlValue
		case .text:
			return String.defaultSqlValue
		case .real:
			return Double.defaultSqlValue
		case .date:
			return Date.defaultSqlValue
		case .uuid:
			return UUID.defaultSqlValue
		case .blob:
			return Data.defaultSqlValue
		}
	}

}
public protocol SqlBuilder {
	func add(name: String, value: Int)
	func add(name: String, value: Int?)
	func add(name: String, value: Bool)
	func add(name: String, value: Bool?)
	func add(name: String, value: String)
	func add(name: String, value: String?)
	func add(name: String, value: Double)
	func add(name: String, value: Double?)
	func add(name: String, value: Date)
	func add(name: String, value: Date?)
	func add(name: String, value: UUID)
	func add(name: String, value: UUID?)
	func add(name: String, value: Data)
	func add(name: String, value: Data?)
	func add(name: String, value: SqlIntRepresentible, type: SqlIntRepresentible.Type)
	func add(name: String, value: SqlIntRepresentible?, type: SqlIntRepresentible.Type)
	func add(name: String, value: SqlStringRepresentible, type: SqlStringRepresentible.Type)
	func add(name: String, value: SqlStringRepresentible?, type: SqlStringRepresentible.Type)
	func addEncoded<T: Encodable>(name: String, value: T)
	func addEncoded<T: Encodable>(name: String, value: T?)
}

class SqlEncoder: Encoder {
	var codingPath: [CodingKey] = []
	
	var userInfo: [CodingUserInfoKey : Any] = [:]
	var builder: SqlBuilder
	
	init(to builder: SqlBuilder) {
		self.builder = builder
	}
	func container<Key>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> where Key : CodingKey {
		let container = KeyedContainer<Key>(to: builder, path: codingPath)
		return KeyedEncodingContainer(container)
	}
	
	func unkeyedContainer() -> UnkeyedEncodingContainer {
		return NilUnkeyedEncodingContainer(codingPath: codingPath, count: 0)
	}
	
	func singleValueContainer() -> SingleValueEncodingContainer {
		return NilSingleValueContainer(codingPath: codingPath)
	}
	
	struct KeyedContainer<Key: CodingKey>: KeyedEncodingContainerProtocol {
		mutating func encodeNil(forKey key: Key) throws {
			throw EncodingError.invalidValue("nil", EncodingError.Context(codingPath: self.codingPath, debugDescription: "nil values not supported"))
		}
		
		//		mutating func encode(_ value: Int16, forKey key: Key) throws {
		//			builder.addColumn(name: key.stringValue, type: .int)
		//		}
		//
		
		mutating func encode(_ value: Bool, forKey key: Key) {
			builder.add(name: key.stringValue, value: value)
		}
		mutating func encode(_ value: Double, forKey key: Key) {
			builder.add(name: key.stringValue, value: value)
		}
		mutating func encode(_ value: Float, forKey key: Key) {
			builder.add(name: key.stringValue, value: Double(value))
		}
		mutating func encode(_ value: Int, forKey key: Key) {
			builder.add(name: key.stringValue, value: value)
		}
		mutating func encode(_ value: Int8, forKey key: Key) {
			builder.add(name: key.stringValue, value: Int(value))
		}
		mutating func encode(_ value: Int16, forKey key: Key) {
			builder.add(name: key.stringValue, value: Int(value))
		}
		mutating func encode(_ value: Int32, forKey key: Key) {
			builder.add(name: key.stringValue, value: Int(value))
		}
		mutating func encode(_ value: Int64, forKey key: Key) {
			builder.add(name: key.stringValue, value: Int(value))
		}
		mutating func encode(_ value: String, forKey key: Key) {
			builder.add(name: key.stringValue, value: value)
		}
		mutating func encode(_ value: UInt, forKey key: Key) {
			builder.add(name: key.stringValue, value: Int(value))
		}
		mutating func encode(_ value: UInt8, forKey key: Key) {
			builder.add(name: key.stringValue, value: Int(value))
		}
		mutating func encode(_ value: UInt16, forKey key: Key) {
			builder.add(name: key.stringValue, value: Int(value))
		}
		mutating func encode(_ value: UInt32, forKey key: Key) {
			builder.add(name: key.stringValue, value: Int(value))
		}
		mutating func encode(_ value: UInt64, forKey key: Key) {
			builder.add(name: key.stringValue, value: Int(value))
		}
		mutating func encodeIfPresent(_ value: Bool?, forKey key: Key) {
			builder.add(name: key.stringValue, value: value)
		}
		mutating func encodeIfPresent(_ value: Double?, forKey key: Key) {
			builder.add(name: key.stringValue, value: value)
		}
		mutating func encodeIfPresent(_ value: Float?, forKey key: Key) {
			builder.add(name: key.stringValue, value: toDouble(value))
		}
		mutating func encodeIfPresent(_ value: Int?, forKey key: Key) {
			builder.add(name: key.stringValue, value: value)
		}
		mutating func encodeIfPresent(_ value: Int8?, forKey key: Key) {
			builder.add(name: key.stringValue, value: toInt(value))
		}
		mutating func encodeIfPresent(_ value: Int16?, forKey key: Key) {
			builder.add(name: key.stringValue, value: toInt(value))
		}
		mutating func encodeIfPresent(_ value: Int32?, forKey key: Key) {
			builder.add(name: key.stringValue, value: toInt(value))
		}
		mutating func encodeIfPresent(_ value: Int64?, forKey key: Key) {
			builder.add(name: key.stringValue, value: toInt(value))
		}
		mutating func encodeIfPresent(_ value: String?, forKey key: Key) {
			builder.add(name: key.stringValue, value: value)
		}
		mutating func encodeIfPresent(_ value: UInt?, forKey key: Key) {
			builder.add(name: key.stringValue, value: toInt(value))
		}
		mutating func encodeIfPresent(_ value: UInt8?, forKey key: Key) {
			builder.add(name: key.stringValue, value: toInt(value))
		}
		mutating func encodeIfPresent(_ value: UInt16?, forKey key: Key) {
			builder.add(name: key.stringValue, value: toInt(value))
		}
		mutating func encodeIfPresent(_ value: UInt32?, forKey key: Key) {
			builder.add(name: key.stringValue, value: toInt(value))
		}
		mutating func encodeIfPresent(_ value: UInt64?, forKey key: Key) {
			builder.add(name: key.stringValue, value: toInt(value))
		}
		
		mutating func encode<T>(_ value: T, forKey key: Key) throws where T : Encodable {
			if T.self == Date.self {
				builder.add(name: key.stringValue, value: value as! Date)
			} else if T.self == UUID.self {
				builder.add(name: key.stringValue, value: value as! UUID)
			} else if T.self == Data.self {
				builder.add(name: key.stringValue, value: value as! Data)
			} else if let intType = T.self as? SqlIntRepresentible.Type {
				builder.add(name: key.stringValue, value: value as! SqlIntRepresentible, type: intType)
			} else if let stringType = T.self as? SqlStringRepresentible.Type {
				builder.add(name: key.stringValue, value: value as! SqlStringRepresentible, type: stringType)
			}  else {
				builder.addEncoded(name: key.stringValue, value: value)
			}
		}
		
		mutating func encodeIfPresent<T>(_ value: T?, forKey key: Key) throws where T : Encodable {
			if T.self == Date.self {
				builder.add(name: key.stringValue, value: value as! Date?)
			} else if T.self == UUID.self {
				builder.add(name: key.stringValue, value: value as! UUID?)
			} else if T.self == Data.self {
				builder.add(name: key.stringValue, value: value as! Data?)
			} else if let intType = T.self as? SqlIntRepresentible.Type {
				builder.add(name: key.stringValue, value: value as! SqlIntRepresentible?, type: intType)
			} else if let stringType = T.self as? SqlStringRepresentible.Type {
				builder.add(name: key.stringValue, value: value as! SqlStringRepresentible?, type: stringType)
			}  else {
				builder.addEncoded(name: key.stringValue, value: value)
			}
		}
		mutating func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type, forKey key: Key) -> KeyedEncodingContainer<NestedKey> where NestedKey : CodingKey {
			return KeyedEncodingContainer(NilKeyedEncodingContainer<NestedKey>(codingPath: codingPath))
		}
		
		mutating func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer {
			return NilUnkeyedEncodingContainer(codingPath: self.codingPath, count: 0)
		}
		
		mutating func superEncoder() -> Encoder {
			return SqlEncoder(to: builder)
		}
		
		mutating func superEncoder(forKey key: Key) -> Encoder {
			return SqlEncoder(to: builder)
		}
		
		private var builder: SqlBuilder
		var codingPath: [CodingKey]
		init(to builder: SqlBuilder, path: [CodingKey]) {
			self.builder = builder
			self.codingPath = path
		}
		
		private func toDouble(_ v: Float?) -> Double? {
			guard let v = v else { return nil }
			return Double(v)
		}
		private func toInt(_ v: Int8?) -> Int? {
			guard let v = v else { return nil }
			return Int(v)
		}
		private func toInt(_ v: Int16?) -> Int? {
			guard let v = v else { return nil }
			return Int(v)
		}
		private func toInt(_ v: Int32?) -> Int? {
			guard let v = v else { return nil }
			return Int(v)
		}
		private func toInt(_ v: Int64?) -> Int? {
			guard let v = v else { return nil }
			return Int(v)
		}
		private func toInt(_ v: UInt?) -> Int? {
			guard let v = v else { return nil }
			return Int(v)
		}
		private func toInt(_ v: UInt8?) -> Int? {
			guard let v = v else { return nil }
			return Int(v)
		}
		private func toInt(_ v: UInt16?) -> Int? {
			guard let v = v else { return nil }
			return Int(v)
		}
		private func toInt(_ v: UInt32?) -> Int? {
			guard let v = v else { return nil }
			return Int(v)
		}
		private func toInt(_ v: UInt64?) -> Int? {
			guard let v = v else { return nil }
			return Int(v)
		}
	}
}
