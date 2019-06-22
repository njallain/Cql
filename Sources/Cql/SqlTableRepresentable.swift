//
//  SqlTableRepresentable.swift
//  Sql
//
//  Created by Neil Allain on 4/20/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

public protocol Initable {
	init()
}

/**
Any Codable object can be used as a table, however implementing SqlTableRepresentable
allows the primary key and other indexes to be specified
*/
public protocol SqlTableRepresentable: Codable, Initable {
	static func buildSchema() -> TableSchemaProtocol
	static var tableIndexes: [TableIndex] {get}
	static var foreignKeys: [ForeignKeyRelation] {get}
	static var sqlCoder: SqlCoder<Self> {get}
}

/**
Protocol to implement if the table has a single primary key
*/
public protocol PrimaryKeyTable: SqlTableRepresentable {
	associatedtype Key: SqlComparable
	static var primaryKey: WritableKeyPath<Self, Key> {get}
}

/**
Protocol to implement if the table has a two part primary key
*/
public protocol PrimaryKeyTable2: SqlTableRepresentable {
	associatedtype Key1: SqlComparable
	associatedtype Key2: SqlComparable
	static var primaryKey: (WritableKeyPath<Self, Key1>, WritableKeyPath<Self, Key2>) {get}
}

public extension SqlTableRepresentable {
	static var sqlCoder: SqlCoder<Self> { SqlCoder<Self>() }
}
//extension SqlTableRepresentable where Self: PrimaryKeyTable {
public extension PrimaryKeyTable {
	static func buildSchema() -> TableSchemaProtocol {
		
		let schema = buildBaseSchema(self)
		if let keyName = SqlPropertyPath.path(Self(), keyPath: Self.primaryKey) {
			schema.primaryKey = [keyName]
		}
		return schema
	}
}

public extension SqlTableRepresentable where Self: PrimaryKeyTable2 {
	static func buildSchema() -> TableSchemaProtocol {
		let schema = buildBaseSchema(self)
		let (path1, path2) = Self.primaryKey
		let t = Self()
		if let key1 = SqlPropertyPath.path(t, keyPath: path1), let key2 = SqlPropertyPath.path(t, keyPath: path2) {
			schema.primaryKey = [key1, key2]
		}
		return schema
	}
}

public extension SqlTableRepresentable {
	static func buildSchema() -> TableSchemaProtocol {
		return buildBaseSchema(self)
	}
	static var tableIndexes: [TableIndex] { return [] }
	static var foreignKeys: [ForeignKeyRelation] { return [] }
	
	
}

fileprivate func buildBaseSchema<T: SqlTableRepresentable>(_ type: T.Type) -> TableSchema<T> {
	let schema = TableSchema(name: String(describing: T.self), newRow: T.init)
	schema.indexes = T.tableIndexes
	schema.foreignKeys = T.foreignKeys.map { $0.buildForeignKey() }
	schema.sqlCoder = T.sqlCoder
	let builder = TableBuilder(schema: schema)
	builder.build()
	return schema
}


public protocol ForeignKeyRelation {
	func buildForeignKey() -> ForeignKey
}
public struct RelationToOne<Source: Codable, Target: PrimaryKeyTable> {
	let keyPath: WritableKeyPath<Source, Target.Key>
	let join: JoinProperty<Source, Target, Target.Key>
	init(_ target: Target.Type, _ keyPath: WritableKeyPath<Source, Target.Key>) {
		self.keyPath = keyPath
		self.join = JoinProperty(left: keyPath, right: Target.primaryKey)
	}
}
extension RelationToOne: ForeignKeyRelation where Source: SqlTableRepresentable {
	public func buildForeignKey() -> ForeignKey {
		guard let keyName = SqlPropertyPath.path(Source(), keyPath: self.keyPath),
			let pkName = SqlPropertyPath.path(Target(), keyPath: Target.primaryKey) else {
			fatalError("could not determine property path for \(self.keyPath)")
		}
		return ForeignKey(columnName: keyName, foreignTable: String(describing:Target.self), foreignColumn: pkName)
	}
}
public struct RelationToMany<Source: PrimaryKeyTable, Target: Codable> {
	let keyPath: WritableKeyPath<Target, Source.Key>
	let join: JoinProperty<Source, Target, Source.Key>
	init(_ keyPath: WritableKeyPath<Target, Source.Key>, _ target: Target.Type) {
		self.keyPath = keyPath
		self.join = JoinProperty(left: Source.primaryKey, right: keyPath)
	}
}

public extension PrimaryKeyTable {
	static func toMany<Target: Codable>(_ keyPath: WritableKeyPath<Target, Key>) -> RelationToMany<Self, Target> {
		return RelationToMany(keyPath, Target.self)
	}
}

public extension Encodable where Self: Decodable {
	static func toOne<Target: PrimaryKeyTable>(_ target: Target.Type, _ keyPath: WritableKeyPath<Self, Target.Key>) -> RelationToOne<Self, Target> {
		let relation = RelationToOne(target, keyPath)
		return relation
	}
}
