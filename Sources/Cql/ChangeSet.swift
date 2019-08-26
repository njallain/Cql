//
//  File.swift
//  
//
//  Created by Neil Allain on 8/25/19.
//

import Foundation

public protocol RowChangeSet {
	associatedtype T: Codable
	mutating func new(initFn: (inout T) -> Void) -> T
	mutating func updated(_ row: T)
	mutating func deleted(_ row: T)
	
	var newRows: [T] {get}
	var updatedRows: [T] {get}
	var deletedRows: [T] {get}
}

public class ChangeSet<T: PrimaryKeyTable>: RowChangeSet {
	private var deleted = [T.Key:T]()
	private var updated = [T.Key:T]()
	private var created = [T.Key:T]()
	private var keyAllocator: AnyKeyAllocator<T.Key>
	
	public init<A: KeyAllocator>(_ keyAllocator: A) where A.Key == T.Key {
		self.keyAllocator = AnyKeyAllocator(keyAllocator)
	}
	public init(_ keyAllocator: AnyKeyAllocator<T.Key>) {
		self.keyAllocator = keyAllocator
	}
	public var newRows: [T] { Array(created.values) }
	public var updatedRows: [T] { Array(updated.values) }
	public var deletedRows: [T] { Array(deleted.values) }
	
	public func new(initFn: (inout T) -> Void) -> T {
		var row = T()
		let key = keyAllocator.next()
		row[keyPath: T.primaryKey] = key
		initFn(&row)
		created[key] = row
		return row
	}
	public func updated(_ row: T) {
		let key = row[keyPath: T.primaryKey]
		if created[key] != nil { created[key] = row }
		else if deleted[key] == nil { updated[key] = row }
	}
	public func deleted(_ row: T) {
		let key = row[keyPath: T.primaryKey]
		created.removeValue(forKey: key)
		updated.removeValue(forKey: key)
		deleted[key] = row
	}
}

public class ChangeSet2<T: PrimaryKeyTable2>: RowChangeSet {
	struct Key: Hashable {
		let key1: T.Key1
		let key2: T.Key2
		init(_ row: T) {
			key1 = row[keyPath: T.primaryKey.0]
			key2 = row[keyPath: T.primaryKey.1]
		}
	}
	private var deleted = [Key:T]()
	private var updated = [Key:T]()
	private var created = [Key:T]()
	public init() {
	}
	public var newRows: [T] { Array(created.values) }
	public var updatedRows: [T] { Array(updated.values) }
	public var deletedRows: [T] { Array(deleted.values) }
	
	public func new(initFn: (inout T) -> Void) -> T {
		var row = T()
		initFn(&row)
		created[Key(row)] = row
		return row
	}
	public func updated(_ row: T) {
		let key = Key(row)
		if created[key] != nil { created[key] = row }
		else if deleted[key] == nil { updated[key] = row }
	}
	public func deleted(_ row: T) {
		let key = Key(row)
		created.removeValue(forKey: key)
		updated.removeValue(forKey: key)
		deleted[key] = row
	}
}
