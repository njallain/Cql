//
//  SqlTableRepresentable.swift
//  Sql
//
//  Created by Neil Allain on 4/20/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

public protocol SqlInitable {
	init()
}

/**
Any Codable object can be used as a table, however implementing SqlTableRepresentable
allows the primary key and other indexes to be specified
*/
public protocol SqlTable: Codable, Identifiable, SqlInitable where ID: SqlComparable {
	//associatedtype Key:SqlComparable = ID
	typealias Key = ID
	var id: ID {get set}
	static func buildSchema() -> TableSchemaProtocol
	static var tableIndexes: [TableIndex] {get}
	static var foreignKeys: [CqlForeignKeyRelation] {get}
	static var sqlCoder: SqlCoder<Self> {get}
	static func keyAllocator(_ connection: StorageConnection) throws -> AnyKeyAllocator<ID>
}


public struct JoinKey<L: SqlComparable, R: SqlComparable>: Hashable {
	public var leftKey: L
	public var rightKey: R
}

public extension SqlTable {
	//static var primaryKey: WritableKeyPath<Self, ID> { \Self.id }
	static var sqlCoder: SqlCoder<Self> { SqlCoder<Self>() }
}

public extension SqlTable {
	static func buildSchema() -> TableSchemaProtocol {
		
		let schema = buildBaseSchema(self)
		schema.primaryKey = ["id"]
		return schema
	}
	static var tableIndexes: [TableIndex] { return [] }
	static var foreignKeys: [CqlForeignKeyRelation] { return [] }
}


fileprivate func buildBaseSchema<T: SqlTable>(_ type: T.Type) -> TableSchema<T> {
	let schema = TableSchema(name: String(describing: T.self), newRow: T.init)
	schema.indexes = T.tableIndexes
	schema.foreignKeys = T.foreignKeys.map { $0.buildForeignKey() }
	schema.sqlCoder = T.sqlCoder
	let builder = TableBuilder(schema: schema)
	builder.build()
	return schema
}


public protocol CqlForeignKeyRelation {
	func buildForeignKey() -> ForeignKey
}
public struct RelationToOne<Source: Codable, Target: SqlTable> {
	let keyPath: WritableKeyPath<Source, Target.Key>
	public let join: JoinProperty<Source, Target, Target.Key>
	init(_ target: Target.Type, _ keyPath: WritableKeyPath<Source, Target.Key>) {
		self.keyPath = keyPath
		self.join = JoinProperty(left: keyPath, right: \Target.id)
	}
}

public struct RelationToOptionalOne<Source: Codable, Target: SqlTable> {
	let keyPath: WritableKeyPath<Source, Target.Key?>
	//public let join: OptionalJoinProperty<Source, Target, Target.Key>
	init(_ target: Target.Type, _ keyPath: WritableKeyPath<Source, Target.Key?>) {
		self.keyPath = keyPath
//		self.join = OptionalJoinProperty(left: keyPath, right: Target.primaryKey)
	}
}

public struct RelationToMany<Source: SqlTable, Target: Codable> {
	let keyPath: WritableKeyPath<Target, Source.Key>
	public let join: JoinProperty<Source, Target, Source.Key>
	init(_ keyPath: WritableKeyPath<Target, Source.Key>, _ target: Target.Type) {
		self.keyPath = keyPath
		self.join = JoinProperty(left: \Source.id, right: keyPath)
	}
}

public struct RelationToOptionalMany<Source: SqlTable, Target: Codable> {
	let keyPath: WritableKeyPath<Target, Source.Key?>
	public let join: OptionalJoinProperty<Source, Target, Source.Key>
	init(_ keyPath: WritableKeyPath<Target, Source.Key?>, _ target: Target.Type) {
		self.keyPath = keyPath
		self.join = OptionalJoinProperty(left: \Source.id, right: keyPath)
	}
}
extension RelationToOne: CqlForeignKeyRelation where Source: SqlTable {
	public func buildForeignKey() -> ForeignKey {
		guard let keyName = SqlPropertyPath.path(Source(), keyPath: self.keyPath),
			let pkName = SqlPropertyPath.path(Target(), keyPath: \Target.id) else {
			fatalError("could not determine property path for \(self.keyPath)")
		}
		return ForeignKey(columnName: keyName, foreignTable: String(describing:Target.self), foreignColumn: pkName)
	}
}
extension RelationToOptionalOne: CqlForeignKeyRelation where Source: SqlTable {
	public func buildForeignKey() -> ForeignKey {
		guard let keyName = SqlPropertyPath.path(Source(), keyPath: self.keyPath),
			let pkName = SqlPropertyPath.path(Target(), keyPath: \Target.id) else {
			fatalError("could not determine property path for \(self.keyPath)")
		}
		return ForeignKey(columnName: keyName, foreignTable: String(describing:Target.self), foreignColumn: pkName)
	}
}


public extension SqlTable {
	static func toMany<Target: Codable>(_ keyPath: WritableKeyPath<Target, Key>) -> RelationToMany<Self, Target> {
		return RelationToMany(keyPath, Target.self)
	}
	static func toMany<Target: Codable>(_ keyPath: WritableKeyPath<Target, Key?>) -> RelationToOptionalMany<Self, Target> {
		return RelationToOptionalMany(keyPath, Target.self)
	}
}

public extension Encodable where Self: Decodable {
	static func toOne<Target: SqlTable>(_ target: Target.Type, _ keyPath: WritableKeyPath<Self, Target.Key>) -> RelationToOne<Self, Target> {
		let relation = RelationToOne(target, keyPath)
		return relation
	}
	static func toOne<Target: SqlTable>(_ target: Target.Type, _ keyPath: WritableKeyPath<Self, Target.Key?>) -> RelationToOptionalOne<Self, Target> {
		let relation = RelationToOptionalOne(target, keyPath)
		return relation
	}
}
