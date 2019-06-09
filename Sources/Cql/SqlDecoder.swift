//
//  SqlDecoder.swift
//  Sql
//
//  Created by Neil Allain on 3/3/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

public protocol SqlReader {
	func getNullableInt(name: String) throws -> Int?
	func getNullableReal(name: String) throws -> Double?
	func getNullableText(name: String) throws -> String?
	func getNullableBool(name: String) throws -> Bool?
	func getNullableDate(name: String) throws -> Date?
	func getNullableUuid(name: String) throws -> UUID?
	func getNullableBlob(name: String) throws -> Data?
	func contains(name: String) throws -> Bool

	func getInt(name: String) throws -> Int
	func getReal(name: String) throws -> Double
	func getText(name: String) throws -> String
	func getBool(name: String) throws -> Bool
	func getDate(name: String) throws -> Date
	func getUuid(name: String) throws -> UUID
	func getBlob(name: String) throws -> Data
}
class SqlDecoder: Decoder {
	var codingPath: [CodingKey]
	let reader: SqlReader
	var userInfo: [CodingUserInfoKey : Any]
	let prefix: String
	init(from reader: SqlReader, prefix: String) {
		codingPath = []
		userInfo = [:]
		self.prefix = prefix
		self.reader = reader
	}
	func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key : CodingKey {
		return KeyedDecodingContainer(KeyedContainer<Key>(from: reader, path: codingPath, prefix: prefix))
	}
	
	func unkeyedContainer() throws -> UnkeyedDecodingContainer {
		return NilUnkeyedDecodingContainer(codingPath: codingPath, count: nil, isAtEnd: true, currentIndex: 0)
	}
	
	func singleValueContainer() throws -> SingleValueDecodingContainer {
		return NilSingleValueDecodingContainer(codingPath: codingPath)
	}
	
	
	struct KeyedContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
		var codingPath: [CodingKey]
		let reader: SqlReader
		var allKeys: [Key]
		let prefix: String
		
		init(from reader: SqlReader, path: [CodingKey], prefix: String) {
			codingPath = path
			allKeys = []
			self.prefix = prefix
			self.reader = reader
		}
		func contains(_ key: Key) -> Bool {
			return true
		}
		
		func decodeNil(forKey key: Key) throws -> Bool {
			return try !reader.contains(name: prefix + key.stringValue)
		}
		
		func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool {
			return try reader.getBool(name: prefix + key.stringValue)
		}
		
		func decode(_ type: String.Type, forKey key: Key) throws -> String {
			return try reader.getText(name: prefix + key.stringValue)
		}
		
		func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
			return try reader.getReal(name: prefix + key.stringValue)
		}
		
		func decode(_ type: Float.Type, forKey key: Key) throws -> Float {
			return Float(try reader.getReal(name: prefix + key.stringValue))
		}
		
		func decode(_ type: Int.Type, forKey key: Key) throws -> Int {
			return try reader.getInt(name: prefix + key.stringValue)
		}
		
		func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 {
			return Int8(try reader.getInt(name: prefix + key.stringValue))
		}
		
		func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 {
			return Int16(try reader.getInt(name: prefix + key.stringValue))
		}
		
		func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 {
			return Int32(try reader.getInt(name: prefix + key.stringValue))
		}
		
		func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 {
			return Int64(try reader.getInt(name: prefix + key.stringValue))
		}
		
		func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt {
			return UInt(try reader.getInt(name: prefix + key.stringValue))
		}
		
		func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 {
			return UInt8(try reader.getInt(name: prefix + key.stringValue))
		}
		
		func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 {
			return UInt16(try reader.getInt(name: prefix + key.stringValue))
		}
		
		func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 {
			return UInt32(try reader.getInt(name: prefix + key.stringValue))
		}
		
		func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 {
			return UInt64(try reader.getInt(name: prefix + key.stringValue))
		}
		
		func decode(_ type: UUID.Type, forKey key: Key) throws -> UUID {
			return try reader.getUuid(name: prefix + key.stringValue)
		}
		func decode(_ type: Date.Type, forKey key: Key) throws -> Date {
			return try reader.getDate(name: prefix + key.stringValue)
		}
		func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T : Decodable {
			if type == UUID.self {
				return try decode(UUID.self, forKey: key) as! T
			}
			if type == Date.self {
				return try decode(Date.self, forKey: key) as! T
			}
			if let intType = type as? SqlIntRepresentible.Type {
				let n = try reader.getInt(name: prefix + key.stringValue)
				return intType.value(for: n) as! T
			}
			if let stringType = type as? SqlStringRepresentible.Type {
				let s = try reader.getText(name: prefix + key.stringValue)
				return stringType.value(for: s) as! T
			}
			let data = try reader.getBlob(name: prefix + key.stringValue)
			if type == Data.self {
				return data as! T
			}
			let json = JSONDecoder()
			do {
				let v = try json.decode(type, from: data)
				return v
			} catch {
				print("\(prefix + key.stringValue) : \(String(describing: type)) \(error.localizedDescription)")
				throw error
			}
		}
		
//		func decodeIfPresent(_ type: String.Type, forKey key: Key) throws -> String? {
//			return try reader.getNullableText(name: key.stringValue)
//		}
		func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
			return KeyedDecodingContainer(NilKeyedDecodingContainer<NestedKey>(codingPath: codingPath, allKeys: []))
		}
		
		func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
			return NilUnkeyedDecodingContainer(codingPath: codingPath, count: nil, isAtEnd: true, currentIndex: 0)
		}
		
		func superDecoder() throws -> Decoder {
			return SqlDecoder(from: reader, prefix: self.prefix)
		}
		
		func superDecoder(forKey key: Key) throws -> Decoder {
			return SqlDecoder(from: reader, prefix: self.prefix)
		}
		
		
	}
}

extension SqlReader {
	func getInt(name: String) throws -> Int {
		return try getNullableInt(name: name)!
	}
	func getReal(name: String) throws -> Double {
		return try getNullableReal(name: name)!
	}
	func getText(name: String) throws -> String {
		return try getNullableText(name: name)!
	}
	func getBool(name: String) throws -> Bool {
		return try getNullableBool(name: name)!
	}
	func getDate(name: String) throws -> Date {
		return try getNullableDate(name: name)!
	}
	func getUuid(name: String) throws -> UUID {
		return try getNullableUuid(name: name)!
	}
	func getBlob(name: String) throws -> Data {
		return try getNullableBlob(name: name)!
	}
}
