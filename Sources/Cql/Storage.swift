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
public protocol Storage {
	/**
	Opens a connection to the storage.  All reading/writing must be done through a connection
	*/
	func open() throws -> StorageConnection
	/**
	Returns the schema for the given class/struct.
	Will return nil in cases where the schema isn't defined
	*/
	func schema<T: Codable>(for tableType: T.Type) -> TableSchema<T>
}

/**
A connection to a given storage through which objects can be found, added, removed and updated
*/
public protocol StorageConnection {
	func beginTransaction() throws -> Transaction
	func insert<T: Codable>(_ row: T) throws
	func insert<T: Codable>(_ rows: [T]) throws
	
	func update<T: Codable>(where: Predicate<T>, set: (inout T) -> Void) throws
	func update<T: PrimaryKeyTable>(_ row: T) throws
	func update<T: PrimaryKeyTable>(_ rows: [T]) throws
	
	func update<T: PrimaryKeyTable2>(_ row: T) throws
	func update<T: PrimaryKeyTable2>(_ rows: [T]) throws
	
	func delete<T: PrimaryKeyTable>(_ row: T) throws
	func delete<T: Codable>(_ predicate: Predicate<T>) throws
	
	func delete<T: PrimaryKeyTable2>(_ row: T) throws
	/**
	Finds pagedBy results at a time.
	This is not asynchronous, it will only return when all results are processed or
	the query is cancelled
	The results function will be called however many times needed to return all results, or until it returns
	false to stop the query.
	*/
	func find<T: Codable>(query: Query<T>, results: ([T]) -> Bool) throws
	
	/**
	Finds pagedBy results at a time.
	This is not asynchronous, it will only return when all results are processed or
	the query is cancelled
	The results function will be called however many times needed to return all results, or until it returns
	false to stop the query.
	*/
	func find<T: SqlJoin>(query: JoinedQuery<T>, results: ([T]) -> Bool) throws
	
	func get<T: PrimaryKeyTable>(_ type: T.Type, _ id: T.Key) throws -> T?
	func get<T: PrimaryKeyTable2>(_ type: T.Type, _ id1: T.Key1, _ id2: T.Key2) throws -> T?
	func nextId<T: PrimaryKeyTable>(_ type: T.Type) throws -> Int where T.Key == Int
}

public extension StorageConnection {
	func insert<T: Codable>(_ row: T) throws {
		try insert([row])
	}
	func update<T: PrimaryKeyTable>(_ row: T) throws {
		try update([row])
	}
	func update<T: PrimaryKeyTable2>(_ row: T) throws {
		try update([row])
	}
	func get<T: PrimaryKeyTable>(_ type: T.Type, _ id: T.Key) throws -> T? {
		let predicate = Where.all(type).property(T.primaryKey, .equal(id))
		let vs = try self.find(predicate)
		return vs.first
	}
	func get<T: PrimaryKeyTable2>(_ type: T.Type, _ id1: T.Key1, _ id2: T.Key2) throws -> T? {
		let predicate = Predicate.all(type)
			.property(T.primaryKey.0, .equal(id1))
			.property(T.primaryKey.1, .equal(id2))
		let vs = try self.find(predicate)
		return vs.first
	}
	func delete<T: PrimaryKeyTable>(_ row: T) throws {
		let predicate = Predicate.all(T.self).property(T.primaryKey, .equal(row[keyPath: T.primaryKey]))
		try delete(predicate)
	}
	func delete<T: PrimaryKeyTable2>(_ row: T) throws {
		let predicate = Predicate.all(T.self)
			.property(T.primaryKey.0, .equal(row[keyPath: T.primaryKey.0]))
			.property(T.primaryKey.1, .equal(row[keyPath: T.primaryKey.1]))
		try delete(predicate)
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
