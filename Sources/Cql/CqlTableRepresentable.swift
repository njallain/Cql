//
//  SqlTableRepresentable.swift
//  Sql
//
//  Created by Neil Allain on 4/20/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

public protocol CqlInitable {
	init()
}

/**
Any Codable object can be used as a table, however implementing SqlTableRepresentable
allows the primary key and other indexes to be specified
*/
public protocol CqlTableRepresentable: Codable, CqlInitable {
	static func buildSchema() -> TableSchemaProtocol
	static var tableIndexes: [TableIndex] {get}
	static var foreignKeys: [CqlForeignKeyRelation] {get}
	static var sqlCoder: SqlCoder<Self> {get}
}

/**
Protocol to implement if the table has a single primary key
*/
public protocol CqlPrimaryKeyTable: CqlTableRepresentable {
	associatedtype Key: SqlComparable
	static func keyAllocator(_ connection: StorageConnection) throws -> AnyKeyAllocator<Key>
	static var primaryKey: WritableKeyPath<Self, Key> {get}
}

/**
Protocol to implement if the table has a two part primary key
*/
public protocol CqlPrimaryKeyTable2: CqlTableRepresentable {
	associatedtype Key1: SqlComparable
	associatedtype Key2: SqlComparable
	static var primaryKey: (WritableKeyPath<Self, Key1>, WritableKeyPath<Self, Key2>) {get}
}

public extension Cql {
	struct JoinKey<L: SqlComparable, R: SqlComparable>: Hashable {
		public var leftKey: L
		public var rightKey: R
	}
}
public extension CqlPrimaryKeyTable2 {
	var primaryKeys: Cql.JoinKey<Key1, Key2> { Cql.JoinKey(leftKey: self[keyPath: Self.primaryKey.0], rightKey: self[keyPath: Self.primaryKey.1]) }
}
public extension CqlTableRepresentable {
	static var sqlCoder: SqlCoder<Self> { SqlCoder<Self>() }
}
//extension SqlTableRepresentable where Self: PrimaryKeyTable {
public extension CqlPrimaryKeyTable {
	static func buildSchema() -> TableSchemaProtocol {
		
		let schema = buildBaseSchema(self)
		if let keyName = SqlPropertyPath.path(Self(), keyPath: Self.primaryKey) {
			schema.primaryKey = [keyName]
		}
		return schema
	}
}

public extension CqlTableRepresentable where Self: CqlPrimaryKeyTable2 {
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

public extension CqlTableRepresentable {
	static func buildSchema() -> TableSchemaProtocol {
		return buildBaseSchema(self)
	}
	static var tableIndexes: [TableIndex] { return [] }
	static var foreignKeys: [CqlForeignKeyRelation] { return [] }
	
	
}

fileprivate func buildBaseSchema<T: CqlTableRepresentable>(_ type: T.Type) -> TableSchema<T> {
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
public extension Cql {
	struct RelationToOne<Source: Codable, Target: CqlPrimaryKeyTable> {
		let keyPath: WritableKeyPath<Source, Target.Key>
		public let join: JoinProperty<Source, Target, Target.Key>
		init(_ target: Target.Type, _ keyPath: WritableKeyPath<Source, Target.Key>) {
			self.keyPath = keyPath
			self.join = JoinProperty(left: keyPath, right: Target.primaryKey)
		}
	}

	struct RelationToOptionalOne<Source: Codable, Target: CqlPrimaryKeyTable> {
		let keyPath: WritableKeyPath<Source, Target.Key?>
		//public let join: OptionalJoinProperty<Source, Target, Target.Key>
		init(_ target: Target.Type, _ keyPath: WritableKeyPath<Source, Target.Key?>) {
			self.keyPath = keyPath
	//		self.join = OptionalJoinProperty(left: keyPath, right: Target.primaryKey)
		}
	}

	struct RelationToMany<Source: CqlPrimaryKeyTable, Target: Codable> {
		let keyPath: WritableKeyPath<Target, Source.Key>
		public let join: JoinProperty<Source, Target, Source.Key>
		init(_ keyPath: WritableKeyPath<Target, Source.Key>, _ target: Target.Type) {
			self.keyPath = keyPath
			self.join = JoinProperty(left: Source.primaryKey, right: keyPath)
		}
	}

	struct RelationToOptionalMany<Source: CqlPrimaryKeyTable, Target: Codable> {
		let keyPath: WritableKeyPath<Target, Source.Key?>
		public let join: OptionalJoinProperty<Source, Target, Source.Key>
		init(_ keyPath: WritableKeyPath<Target, Source.Key?>, _ target: Target.Type) {
			self.keyPath = keyPath
			self.join = OptionalJoinProperty(left: Source.primaryKey, right: keyPath)
		}
	}
}
extension Cql.RelationToOne: CqlForeignKeyRelation where Source: CqlTableRepresentable {
	public func buildForeignKey() -> ForeignKey {
		guard let keyName = SqlPropertyPath.path(Source(), keyPath: self.keyPath),
			let pkName = SqlPropertyPath.path(Target(), keyPath: Target.primaryKey) else {
			fatalError("could not determine property path for \(self.keyPath)")
		}
		return ForeignKey(columnName: keyName, foreignTable: String(describing:Target.self), foreignColumn: pkName)
	}
}
extension Cql.RelationToOptionalOne: CqlForeignKeyRelation where Source: CqlTableRepresentable {
	public func buildForeignKey() -> ForeignKey {
		guard let keyName = SqlPropertyPath.path(Source(), keyPath: self.keyPath),
			let pkName = SqlPropertyPath.path(Target(), keyPath: Target.primaryKey) else {
			fatalError("could not determine property path for \(self.keyPath)")
		}
		return ForeignKey(columnName: keyName, foreignTable: String(describing:Target.self), foreignColumn: pkName)
	}
}


public extension CqlPrimaryKeyTable {
	static func toMany<Target: Codable>(_ keyPath: WritableKeyPath<Target, Key>) -> Cql.RelationToMany<Self, Target> {
		return Cql.RelationToMany(keyPath, Target.self)
	}
	static func toMany<Target: Codable>(_ keyPath: WritableKeyPath<Target, Key?>) -> Cql.RelationToOptionalMany<Self, Target> {
		return Cql.RelationToOptionalMany(keyPath, Target.self)
	}
}

public extension Encodable where Self: Decodable {
	static func toOne<Target: CqlPrimaryKeyTable>(_ target: Target.Type, _ keyPath: WritableKeyPath<Self, Target.Key>) -> Cql.RelationToOne<Self, Target> {
		let relation = Cql.RelationToOne(target, keyPath)
		return relation
	}
	static func toOne<Target: CqlPrimaryKeyTable>(_ target: Target.Type, _ keyPath: WritableKeyPath<Self, Target.Key?>) -> Cql.RelationToOptionalOne<Self, Target> {
		let relation = Cql.RelationToOptionalOne(target, keyPath)
		return relation
	}
}