//
//  File.swift
//  
//
//  Created by Neil Allain on 6/17/19.
//

import Foundation

public struct Query<T: Codable> {
	let predicate: Predicate<T>
	var pageSize: Int = Int.max
	var order: Order<T>? = nil
	
}

public struct JoinedQuery<T: Codable, U: Codable> {
	let predicate: JoinedPredicate<T,U>
	var pageSize: Int = Int.max
	var order: JoinedOrder<T, U>? = nil
}


public extension StorageConnection {
//	func find<T: Codable>(_ predicate: Predicate<T>, pagedBy: Int, results: ([T]) -> Bool) throws
//	func find<T1: Codable, T2: Codable>(_ predicate: JoinedPredicate<T1, T2>, pagedBy: Int, results: ([(T1,T2)]) -> Bool) throws
	func find<T: Codable>(_ predicate: Predicate<T>) throws -> [T] {
		var results: [T]? = nil
		try self.find(query: Query(predicate: predicate, pageSize: Int.max)) {
			results = $0
			return true
		}
		return results ?? []
	}
	func find<T: Codable>(_ query: Query<T>) throws -> [T] {
		var results: [T]? = nil
		try self.find(query: query) {
			results = $0
			return true
		}
		return results ?? []
	}
	func find<T: Codable, U: Codable>(_ query: JoinedQuery<T,U>) throws -> [(T,U)] {
		var results: [(T,U)]? = nil
		try self.find(query: query) {
			results = $0
			return true
		}
		return results ?? []
	}
	func find<T1: Codable, T2: Codable>(_ predicate: JoinedPredicate<T1, T2>) throws -> [(T1,T2)] {
		var results: [(T1, T2)]?
		try self.find(query: JoinedQuery(predicate: predicate, pageSize: Int.max)) {
			results = $0
			return true
		}
		return results ?? []
	}
	func find<T: PrimaryKeyTable, U: Codable>(_ relationship: RelationToMany<T, U>, of parent: T) throws -> [U] {
		let id = parent[keyPath: T.primaryKey]
		let predicate = Where.all(U.self).property(relationship.keyPath, .equal(id))
		return try self.find(predicate)
	}
//	func find<T: PrimaryKeyTable, U: Codable>(_ relationship: RelationToMany<T, U>, of parents: Predicate<T>) throws -> [U] {
//		return []
//	}

}
