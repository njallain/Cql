//
//  NilEncoder.swift
//  Sql
//
//  Created by Neil Allain on 3/2/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

/**
An an encoder that does nothing
*/
struct NilEncoder: Encoder {
	var codingPath: [CodingKey]
	
	var userInfo: [CodingUserInfoKey : Any]
	
	func container<Key>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> where Key : CodingKey {
		return KeyedEncodingContainer(NilKeyedEncodingContainer<Key>(codingPath: []))
	}
	
	func unkeyedContainer() -> UnkeyedEncodingContainer {
		return NilUnkeyedEncodingContainer(codingPath: codingPath, count: 0)
	}
	
	func singleValueContainer() -> SingleValueEncodingContainer {
		return NilSingleValueContainer(codingPath: codingPath)
	}
	
	init(codingPath: [CodingKey]) {
		self.codingPath = codingPath
		self.userInfo = [:]
	}
	
}
struct NilKeyedEncodingContainer<Key: CodingKey>: KeyedEncodingContainerProtocol {
	
	mutating func encodeNil(forKey key: Key) throws {
	}
	
	mutating func encode(_ value: Int16, forKey key: Key) throws {
	}
	
	mutating func encode(_ value: Int32, forKey key: Key) throws {
	}
	
	mutating func encode(_ value: Int64, forKey key: Key) throws {
	}
	
	mutating func encode(_ value: UInt, forKey key: Key) throws {
	}
	
	mutating func encode(_ value: UInt8, forKey key: Key) throws {
	}
	
	mutating func encode(_ value: UInt16, forKey key: Key) throws {
	}
	
	mutating func encode(_ value: UInt32, forKey key: Key) throws {
	}
	
	mutating func encode(_ value: UInt64, forKey key: Key) throws {
	}
	
	
	mutating func encode<T>(_ value: T, forKey key: Key) throws where T : Encodable {
	}
	
	mutating func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type, forKey key: Key) -> KeyedEncodingContainer<NestedKey> where NestedKey : CodingKey {
		return KeyedEncodingContainer(NilKeyedEncodingContainer<NestedKey>(codingPath: self.codingPath))
	}
	
	mutating func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer {
		return NilUnkeyedEncodingContainer(codingPath: codingPath, count: 0)
	}
	
	mutating func superEncoder() -> Encoder {
		return NilEncoder(codingPath: codingPath)
	}
	
	mutating func superEncoder(forKey key: Key) -> Encoder {
		return NilEncoder(codingPath: codingPath)
	}
	
	var codingPath: [CodingKey]
}

struct NilUnkeyedEncodingContainer: UnkeyedEncodingContainer {
	mutating func encode(_ value: String) throws {
	}
	
	mutating func encode(_ value: Double) throws {
	}
	
	mutating func encode(_ value: Float) throws {
	}
	
	mutating func encode(_ value: Int) throws {
	}
	
	mutating func encode(_ value: Int8) throws {
	}
	
	mutating func encode(_ value: Int16) throws {
	}
	
	mutating func encode(_ value: Int32) throws {
	}
	
	mutating func encode(_ value: Int64) throws {
	}
	
	mutating func encode(_ value: UInt) throws {
	}
	
	mutating func encode(_ value: UInt8) throws {
	}
	
	mutating func encode(_ value: UInt16) throws {
	}
	
	mutating func encode(_ value: UInt32) throws {
	}
	
	mutating func encode(_ value: UInt64) throws {
	}
	
	mutating func encode<T>(_ value: T) throws where T : Encodable {
	}
	
	mutating func encode(_ value: Bool) throws {
	}
	
	var codingPath: [CodingKey]
	
	var count: Int
	
	mutating func encodeNil() throws {
	}
	
	mutating func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type) -> KeyedEncodingContainer<NestedKey> where NestedKey : CodingKey {
		return KeyedEncodingContainer(NilKeyedEncodingContainer<NestedKey>(codingPath: self.codingPath))
	}
	
	mutating func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
		return self
	}
	
	mutating func superEncoder() -> Encoder {
		return NilEncoder(codingPath: self.codingPath)
	}
}

/**
For table schemas, the values themselves aren't important
*/
struct NilSingleValueContainer: SingleValueEncodingContainer {
	var codingPath: [CodingKey]
	mutating func encodeNil() throws {
	}
	
	mutating func encode(_ value: Bool) throws {
	}
	
	mutating func encode(_ value: String) throws {
	}
	
	mutating func encode(_ value: Double) throws {
	}
	
	mutating func encode(_ value: Float) throws {
	}
	
	mutating func encode(_ value: Int) throws {
	}
	
	mutating func encode(_ value: Int8) throws {
	}
	
	mutating func encode(_ value: Int16) throws {
	}
	
	mutating func encode(_ value: Int32) throws {
	}
	
	mutating func encode(_ value: Int64) throws {
	}
	
	mutating func encode(_ value: UInt) throws {
	}
	
	mutating func encode(_ value: UInt8) throws {
	}
	
	mutating func encode(_ value: UInt16) throws {
	}
	
	mutating func encode(_ value: UInt32) throws {
	}
	
	mutating func encode(_ value: UInt64) throws {
	}
	
	mutating func encode<T>(_ value: T) throws where T : Encodable {
	}
	
	
}

