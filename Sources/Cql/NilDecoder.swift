//
//  NilDecoder.swift
//  Sql
//
//  Created by Neil Allain on 3/3/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

struct NilDecoder: Decoder {
	var codingPath: [CodingKey]
	var userInfo: [CodingUserInfoKey : Any]
	init() {
		codingPath = []
		userInfo = [:]
	}
	func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key : CodingKey {
		return KeyedDecodingContainer(NilKeyedDecodingContainer<Key>(codingPath: codingPath, allKeys: []))
	}
	
	func unkeyedContainer() throws -> UnkeyedDecodingContainer {
		return NilUnkeyedDecodingContainer(codingPath: codingPath, count: 0, isAtEnd: true, currentIndex: 0)
	}
	
	func singleValueContainer() throws -> SingleValueDecodingContainer {
		return NilSingleValueDecodingContainer(codingPath: codingPath)
	}
	
	
}
struct NilKeyedDecodingContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
	var codingPath: [CodingKey]
	
	var allKeys: [Key]
	
	func contains(_ key: Key) -> Bool {
		return false
	}
	
	func decodeNil(forKey key: Key) throws -> Bool {
		return false
	}
	
	func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool {
		return false
	}
	
	func decode(_ type: String.Type, forKey key: Key) throws -> String {
		return ""
	}
	
	func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
		return 0
	}
	
	func decode(_ type: Float.Type, forKey key: Key) throws -> Float {
		return 0
	}
	
	func decode(_ type: Int.Type, forKey key: Key) throws -> Int {
		return 0
	}
	
	func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 {
		return 0
	}
	
	func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 {
		return 0
	}
	
	func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 {
		return 0
	}
	
	func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 {
		return 0
	}
	
	func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt {
		return 0
	}
	
	func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 {
		return 0
	}
	
	func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 {
		return 0
	}
	
	func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 {
		return 0
	}
	
	func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 {
		return 0
	}
	
	func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T : Decodable {
		throw DecodingError.valueNotFound(type, DecodingError.Context(codingPath: self.codingPath, debugDescription: "not supported"))
	}
	
	func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
		return KeyedDecodingContainer(NilKeyedDecodingContainer<NestedKey>(codingPath: codingPath, allKeys: []))
	}
	
	func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
		return NilUnkeyedDecodingContainer(codingPath: codingPath, count: nil, isAtEnd: true, currentIndex: 0)
	}
	
	func superDecoder() throws -> Decoder {
		return NilDecoder()
	}
	
	func superDecoder(forKey key: Key) throws -> Decoder {
		return NilDecoder()
	}
	
	
}

struct NilUnkeyedDecodingContainer: UnkeyedDecodingContainer {
	var codingPath: [CodingKey]
	
	var count: Int?
	
	var isAtEnd: Bool
	
	var currentIndex: Int
	
	mutating func decodeNil() throws -> Bool {
		return false
	}
	
	mutating func decode(_ type: Bool.Type) throws -> Bool {
		return false
	}
	
	mutating func decode(_ type: String.Type) throws -> String {
		return ""
	}
	
	mutating func decode(_ type: Double.Type) throws -> Double {
		return 0
	}
	
	mutating func decode(_ type: Float.Type) throws -> Float {
		return 0
	}
	
	mutating func decode(_ type: Int.Type) throws -> Int {
		return 0
	}
	
	mutating func decode(_ type: Int8.Type) throws -> Int8 {
		return 0
	}
	
	mutating func decode(_ type: Int16.Type) throws -> Int16 {
		return 0
	}
	
	mutating func decode(_ type: Int32.Type) throws -> Int32 {
		return 0
	}
	
	mutating func decode(_ type: Int64.Type) throws -> Int64 {
		return 0
	}
	
	mutating func decode(_ type: UInt.Type) throws -> UInt {
		return 0
	}
	
	mutating func decode(_ type: UInt8.Type) throws -> UInt8 {
		return 0
	}
	
	mutating func decode(_ type: UInt16.Type) throws -> UInt16 {
		return 0
	}
	
	mutating func decode(_ type: UInt32.Type) throws -> UInt32 {
		return 0
	}
	
	mutating func decode(_ type: UInt64.Type) throws -> UInt64 {
		return 0
	}
	
	mutating func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
		throw DecodingError.valueNotFound(type, DecodingError.Context(codingPath: self.codingPath, debugDescription: "not supported"))
	}
	
	mutating func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
		return KeyedDecodingContainer(NilKeyedDecodingContainer<NestedKey>(codingPath: codingPath, allKeys: []))
	}
	
	mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
		return NilUnkeyedDecodingContainer(codingPath: codingPath, count: nil, isAtEnd: true, currentIndex: 0)
	}
	
	mutating func superDecoder() throws -> Decoder {
		return NilDecoder()
	}
	
	
}


struct NilSingleValueDecodingContainer: SingleValueDecodingContainer {
	var codingPath: [CodingKey]
	
	func decodeNil() -> Bool {
		return false
	}
	
	func decode(_ type: Bool.Type) throws -> Bool {
		return false
	}
	
	func decode(_ type: String.Type) throws -> String {
		return ""
	}
	
	func decode(_ type: Double.Type) throws -> Double {
		return 0
	}
	
	func decode(_ type: Float.Type) throws -> Float {
		return 0
	}
	
	func decode(_ type: Int.Type) throws -> Int {
		return 0
	}
	
	func decode(_ type: Int8.Type) throws -> Int8 {
		return 0
	}
	
	func decode(_ type: Int16.Type) throws -> Int16 {
		return 0
	}
	
	func decode(_ type: Int32.Type) throws -> Int32 {
		return 0
	}
	
	func decode(_ type: Int64.Type) throws -> Int64 {
		return 0
	}
	
	func decode(_ type: UInt.Type) throws -> UInt {
		return 0
	}
	
	func decode(_ type: UInt8.Type) throws -> UInt8 {
		return 0
	}
	
	func decode(_ type: UInt16.Type) throws -> UInt16 {
		return 0
	}
	
	func decode(_ type: UInt32.Type) throws -> UInt32 {
		return 0
	}
	
	func decode(_ type: UInt64.Type) throws -> UInt64 {
		return 0
	}
	
	func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
		throw DecodingError.valueNotFound(type, DecodingError.Context(codingPath: self.codingPath, debugDescription: "not supported"))
	}
	
	
}
