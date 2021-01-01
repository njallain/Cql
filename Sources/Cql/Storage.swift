//
//  Storage.swift
//  Sql
//
//  Created by Neil Allain on 4/20/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

/**
Indicates a single place where any codable objects are stored
*/
public protocol Storage: ChangeSetSource {
	/**
	Opens a connection to the storage.  All reading/writing must be done through a connection
	*/
	func open() throws -> StorageConnection
	/**
	Returns the schema for the given class/struct.
	Will return nil in cases where the schema isn't defined
	*/
	func schema<T: Codable>(for tableType: T.Type) -> TableSchema<T>
	func keyAllocator<T: SqlTable>(for type: T.Type) -> AnyKeyAllocator<T.Key>
}

public extension Storage {
	func changeSet<T: SqlTable>(for: T.Type) -> ChangeSet<T> {
		return Cql.ChangeSet(self.keyAllocator(for: T.self))
	}
}

/**
A connection to a given storage through which objects can be found, added, removed and updated
*/
public protocol StorageConnection {
	var storage: Storage {get}
	func beginTransaction() throws -> Transaction
	func insert<T: Codable>(_ row: T) throws
	func insert<T: Codable>(_ rows: [T]) throws
	
	func update<T: Codable>(where: Predicate<T>, set: (inout T) -> Void) throws
	func update<T: SqlTable>(_ row: T) throws
	func update<T: SqlTable>(_ rows: [T]) throws
	
	func delete<T: SqlTable>(_ row: T) throws
	func delete<T: Codable>(_ predicate: Predicate<T>) throws
	
	/**
	Finds pagedBy results at a time.
	This is not asynchronous, it will only return when all results are processed or
	the query is cancelled
	The results function will be called however many times needed to return all results, or until it returns
	false to stop the query.
	*/
	func fetch<T: Codable>(query: Query<T>, results: ([T]) -> Bool) throws
	
	/**
	Finds pagedBy results at a time.
	This is not asynchronous, it will only return when all results are processed or
	the query is cancelled
	The results function will be called however many times needed to return all results, or until it returns
	false to stop the query.
	*/
	func fetch<T: AnyJoin>(query: JoinedQuery<T>, results: ([T]) -> Bool) throws
	
	func get<T: SqlTable>(_ type: T.Type, _ id: T.Key) throws -> T?
	func nextId<T: SqlTable>(_ type: T.Type) throws -> Int where T.Key == Int
}

public extension StorageConnection {
	func insert<T: Codable>(_ row: T) throws {
		try insert([row])
	}
	func update<T: SqlTable>(_ row: T) throws {
		try update([row])
	}
	func get<T: SqlTable>(_ type: T.Type, _ id: T.Key) throws -> T? {
		let predicate = \T.id %== id
		let vs = try self.find(predicate)
		return vs.first
	}
	func delete<T: SqlTable>(_ row: T) throws {
		let predicate = \T.id %== row.id
		try delete(predicate)
	}
	func save(changeSets: ChangeSetProtocol...) throws {
		for changeSet in changeSets.reversed() {
			try changeSet.saveDeleted(connection: self)
		}
		for changeSet in changeSets {
			try changeSet.saveNew(connection: self)
		}
		for changeSet in changeSets {
			try changeSet.saveUpdated(connection: self)
		}
	}
}

public class Transaction {
	private var commitFunc: () throws -> Void
	private var rollbackFunc: () throws -> Void
	private var isOpen = true;
	
	init(commit: @escaping () throws -> Void, rollback: @escaping () throws -> Void) {
		self.commitFunc = commit
		self.rollbackFunc = rollback
	}
	public func commit() throws {
		if isOpen {
			try commitFunc()
			isOpen = false
		}
	}
	public func rollback() throws {
		if isOpen {
			try rollbackFunc()
			isOpen = false
		}
	}
	deinit {
		if isOpen {
			do {
				try rollback()
			} catch {
				// drop error?
			}
		}
	}
}

