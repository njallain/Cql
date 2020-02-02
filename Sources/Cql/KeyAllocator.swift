//
//  File.swift
//  
//
//  Created by Neil Allain on 8/25/19.
//

import Foundation

public protocol KeyAllocator {
	associatedtype Key
	func next() -> Key
}
public class IntKeyAllocator: KeyAllocator {
	public typealias Key = Int
	private var nextKey: Int
	public init(_ nextKey: Int) {
		self.nextKey = nextKey
	}
	public func next() -> Int {
		defer { nextKey += 1 }
		return nextKey
	}
}
public struct UuidKeyAllocator: KeyAllocator {
	public typealias Key = UUID
	public func next() -> UUID {
		return UUID()
	}
}
public struct StringKeyAllocator: KeyAllocator {
	public typealias Key = String
	public func next() -> String {
		return UUID().uuidString
	}
}
public struct AnyKeyAllocator<Key>: KeyAllocator {
	private let nextKeyFn: () -> Key
	public init<T: KeyAllocator>(_ allocator: T) where T.Key == Key {
		self.nextKeyFn = allocator.next
	}
	public func next() -> Key {
		return nextKeyFn()
	}
}

public extension CqlPrimaryKeyTable where Key == Int {
	static func keyAllocator(_ connection: StorageConnection) throws -> AnyKeyAllocator<Key> {
		let nextKey = try connection.nextId(self)
		return AnyKeyAllocator(IntKeyAllocator(nextKey))
	}
}

public extension CqlPrimaryKeyTable where Key == UUID {
	static func keyAllocator(_ connection: StorageConnection) throws -> AnyKeyAllocator<Key> {
		return AnyKeyAllocator(UuidKeyAllocator())
	}
}
public extension CqlPrimaryKeyTable where Key == String {
	static func keyAllocator(_ connection: StorageConnection) throws -> AnyKeyAllocator<Key> {
		return AnyKeyAllocator(StringKeyAllocator())
	}
}
