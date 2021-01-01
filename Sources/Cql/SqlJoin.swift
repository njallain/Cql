//
//  SqlJoin.swift
//  
//
//  Created by Neil Allain on 6/20/19.
//

import Foundation

public protocol AnyJoin: Codable, SqlInitable {
	associatedtype Left: Codable
	associatedtype Right: Codable
	static var left: WritableKeyPath<Self, Left> {get}
	static var right: WritableKeyPath<Self, Right> {get}
	static func leftName(_ row: Self.Left) -> String
	static func rightName(_ row: Self.Right) -> String
}

extension AnyJoin {
	init(left: Self.Left, right: Self.Right) {
		self.init()
		self[keyPath: Self.left] = left
		self[keyPath: Self.right] = right
	}
}
/**
Implement this to define a joined query. 
*/
public protocol SqlJoin: AnyJoin {
	associatedtype Property: SqlComparable
	static var relationship: JoinProperty<Left, Right, Property> {get}
}
public struct JoinProperty<Left: Codable, Right: Codable, Property: SqlComparable> {
	var left: WritableKeyPath<Left, Property>
	var right: WritableKeyPath<Right, Property>
}

public extension SqlJoin {
	static func leftName(_ row: Self.Left) -> String {
		guard let n = SqlPropertyPath.path(Self(), keyPath: Self.left, value: row, valueKeyPath: Self.relationship.left) else {
			fatalError("could not determine path name for \(Self.left)")
		}
		return n
	}
	static func rightName(_ row: Self.Right) -> String {
		guard let n = SqlPropertyPath.path(Self(), keyPath: Self.right, value: row, valueKeyPath: Self.relationship.right) else {
			fatalError("could not determine path name for \(Self.right)")
		}
		return n
	}
}

public struct OptionalJoinProperty<Left: Codable, Right: Codable, Property: SqlComparable> {
	var left: WritableKeyPath<Left, Property>
	var right: WritableKeyPath<Right, Property?>
}


public protocol OptionalSqlJoin: AnyJoin {
	associatedtype Property: SqlComparable
	static var relationship: OptionalJoinProperty<Left, Right, Property> {get}
}


public extension OptionalSqlJoin {
	static func leftName(_ row: Self.Left) -> String {
		guard let n = SqlPropertyPath.path(Self(), keyPath: Self.left, value: row, valueKeyPath: Self.relationship.left) else {
			fatalError("could not determine path name for \(Self.left)")
		}
		return n
	}
	static func rightName(_ row: Self.Right) -> String {
		guard let n = SqlPropertyPath.optionalPath(Self(), keyPath: Self.right, value: row, valueKeyPath: Self.relationship.right) else {
			fatalError("could not determine path name for \(Self.right)")
		}
		return n
	}
}

//public struct AnyJoin<Left: Codable, Right: Codable, Property: SqlComparable> {
//	init<T: SqlJoin>(join: T) where T.Left == Left, T.Right == Right, T.Property == Property {
//		getLeft = { join[keyPath: T.left] }
//		getRight = { join[keyPath: T.right] }
//	}
//	public var leftSide
//	private var getLeft: () -> Left
//	private var getRight: () -> Right
//}
