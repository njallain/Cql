//
//  SqlJoin.swift
//  
//
//  Created by Neil Allain on 6/20/19.
//

import Foundation

/**
Implement this to define a joined query. 
*/
public protocol SqlJoin: Codable, Initable {
	associatedtype Left: Codable
	associatedtype Right: Codable
	associatedtype Property: SqlComparable
	static var relationship: JoinProperty<Left, Right, Property> {get}
	static var left: WritableKeyPath<Self, Left> {get}
	static var right: WritableKeyPath<Self, Right> {get}
}
public struct JoinProperty<Left: Codable, Right: Codable, Property: SqlComparable> {
	var left: WritableKeyPath<Left, Property>
	var right: WritableKeyPath<Right, Property>
}

extension SqlJoin {
	init(left: Self.Left, right: Self.Right) {
		self.init()
		self[keyPath: Self.left] = left
		self[keyPath: Self.right] = right
	}
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


public protocol OptionalSqlJoin: Codable, Initable {
	associatedtype Left: Codable
	associatedtype Right: Codable
	associatedtype Property: SqlComparable
	static var relationship: OptionalJoinProperty<Left, Right, Property> {get}
	static var left: WritableKeyPath<Self, Left> {get}
	static var right: WritableKeyPath<Self, Right> {get}
}


extension OptionalSqlJoin {
	init(left: Self.Left, right: Self.Right) {
		self.init()
		self[keyPath: Self.left] = left
		self[keyPath: Self.right] = right
	}
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
