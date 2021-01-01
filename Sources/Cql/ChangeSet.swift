//
//  File.swift
//  
//
//  Created by Neil Allain on 8/25/19.
//

import Foundation

/**
Represents anything that can be stored.  This could be a change set for single table, or change sets for multiple tables
*/
public protocol Storable {
	func save(to connection: StorageConnection) throws
}

/**
Protocol for change sets.  This is a protocol without an associated table type so change sets of different tables
can be grouped together in a save.
*/
public protocol ChangeSetProtocol {
	func saveNew(connection: StorageConnection) throws
	func saveUpdated(connection: StorageConnection) throws
	func saveDeleted(connection: StorageConnection) throws
}

public protocol RowChangeSet: ChangeSetProtocol {
	associatedtype T: Codable
	mutating func new(initializer: (inout T) -> Void) -> T
	mutating func updated(_ row: T)
	mutating func deleted(_ row: T)
	
	var newRows: [T] {get}
	var updatedRows: [T] {get}
	var deletedRows: [T] {get}
}

public extension RowChangeSet {
	func saveNew(connection: StorageConnection) throws {
		try connection.insert(newRows)
	}
}

public class ChangeSet<T: SqlTable>: RowChangeSet {
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
	
	@discardableResult
	public func new(initializer: (inout T) -> Void) -> T {
		var row = T()
		let key = keyAllocator.next()
		row.id = key
		initializer(&row)
		created[key] = row
		return row
	}
	public func updated(_ row: T) {
		let key = row.id
		if created[key] != nil { created[key] = row }
		else if deleted[key] == nil { updated[key] = row }
	}
	public func deleted(_ row: T) {
		let key = row.id
		created.removeValue(forKey: key)
		updated.removeValue(forKey: key)
		deleted[key] = row
	}
	public func saveUpdated(connection: StorageConnection) throws {
		try connection.update(self.updatedRows)
	}
	public func saveDeleted(connection: StorageConnection) throws {
		for row in deletedRows { try connection.delete(row) }
	}
}


public protocol ChangeSetSource {
	func changeSet<T: SqlTable>(for type: T.Type) -> ChangeSet<T>
}

