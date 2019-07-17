//
//  File.swift
//  
//
//  Created by Neil Allain on 6/17/19.
//

import Foundation

public struct Query<T: Codable> {
	public init(predicate: Predicate<T>, pageSize: Int = Int.max, order: Order<T>? = nil) {
		self.predicate = predicate
		self.pageSize = pageSize
		self.order = order
	}
	public let predicate: Predicate<T>
	public let pageSize: Int
	public let order: Order<T>?
	
}

public struct JoinedQuery<T: SqlJoin> {
	public init(_ type: T.Type, left: Predicate<T.Left>, right: Predicate<T.Right>, pageSize: Int = Int.max, order: Order<T>? = nil) {
//		let joinExpr = AnyJoinExpression(JoinExpression(left: Model.primaryKey, right: relationship.keyPath))
//		return JoinedPredicate(joinExpressions: [joinExpr], leftPredicate: self, rightPredicate: predicate)
		let joinExpr = AnyJoinExpression(JoinExpression(left: type.relationship.left, right: type.relationship.right))
		self.predicate = JoinedPredicate(joinExpressions: [joinExpr], leftPredicate: left, rightPredicate: right)
		self.pageSize = pageSize
		self.order = order
	}
	let predicate: JoinedPredicate<T.Left,T.Right>
	public let pageSize: Int
	public let order: Order<T>?
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
	func find<T: SqlJoin>(_ query: JoinedQuery<T>) throws -> [T] {
		var results: [T]? = nil
		try self.find(query: query) {
			results = $0
			return true
		}
		return results ?? []
	}
	func findRelated<T: PrimaryKeyTable, U: Codable>(_ relationship: RelationToMany<T, U>, of parent: T) throws -> [U] {
		let id = parent[keyPath: T.primaryKey]
		let predicate = relationship.keyPath %== id 
		return try self.find(predicate)
	}
//	func find<T: PrimaryKeyTable, U: Codable>(_ relationship: RelationToMany<T, U>, of parents: Predicate<T>) throws -> [U] {
//		return []
//	}

}
