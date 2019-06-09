//
//  SqlPredicateBuilder.swift
//  Sql
//
//  Created by Neil Allain on 4/13/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

public struct CompiledSql {
	let fullSql: String
	let selectColumns: [String]
	let whereClause: String
	let arguments: [SqlArgument]
}
protocol SqlCompiler: AnyObject {
	var arguments: [SqlArgument] {get set}
	func childCompiler<C: Codable>(for type: C.Type) -> SqlPredicateCompiler<C>
}
public class SqlPredicateCompiler<T: Codable>: SqlCompiler {
	let database: Storage
	private let aliasId: Int
	private let useAlias: Bool
	var tableAlias: String { return useAlias ? "t\(aliasId)" : "" }
	var tablePrefix: String { return useAlias ? "t\(aliasId)." : "" }
	var argPrefix: String { return "t\(aliasId)arg" }
	let table: TableSchema<T>
	var arguments = [SqlArgument]()
	
	init(database: Storage, aliasId: Int = 0, useAlias: Bool = true) {
		self.database = database
		self.aliasId = aliasId
		self.useAlias = useAlias
		self.table = database.schema(for: T.self)!
	}
	
	func childCompiler<C: Codable>(for type: C.Type) -> SqlPredicateCompiler<C> {
		return SqlPredicateCompiler<C>(database: self.database, aliasId: self.aliasId + 1)
	}
	func name<V: SqlConvertible>(for keyPath: WritableKeyPath<T,V>) -> String {
		guard let n = table.column(keyPath: keyPath) else {
			fatalError("could not find column for key path: \(keyPath), this may be caused by attempting to query on a property that isn't SqlConvertible")
			//throw DatabaseError("could not find column for key path: \(keyPath)")
		}
		return "\(tablePrefix)\(n.name)"
	}
	func compile(_ predicate: Predicate<T>) -> CompiledSql {
		let colNames = table.columns.map({ "\(tablePrefix)\($0.name)" })
		let colSql = colNames.joined(separator: ", ")
		let whereClause = predicate.sql(compiler: self)
		let whereSql = whereClause.isEmpty ? "" : "where " + whereClause
		let fullSql = "select \(colSql) from \(table.name) as \(tableAlias) \(whereSql)"
		return CompiledSql(fullSql: fullSql, selectColumns: colNames, whereClause: whereClause, arguments: self.arguments)
	}

	func compile<T2: Codable>(_ predicate: JoinedPredicate<T, T2>) -> (CompiledSql, SqlPredicateCompiler<T2>) {
		let leftSql = self.compile(predicate.leftPredicate)
		let rightCompiler = self.childCompiler(for: T2.self)
		let rightSql = rightCompiler.compile(predicate.rightPredicate)
		let joins = predicate.joinExpressions.map {
			"\($0.leftName(self)) = \($0.rightName(rightCompiler))"
		}
		let whereClause = [leftSql.whereClause, rightSql.whereClause].sqlJoinedGroups(separator: " and ")
		let whereSql = whereClause.isEmpty ? "" : " where " + whereClause
		let joinSql = joins.sqlJoinedGroups(separator: " and ")
		let cols = leftSql.selectColumns + rightSql.selectColumns
		let colSql = cols.joined(separator: ", ")
		let fullSql = "select \(colSql) from \(table.name) as \(tableAlias) join \(rightCompiler.table.name) as \(rightCompiler.tableAlias) on \(joinSql)\(whereSql)"
		return (CompiledSql(fullSql: fullSql, selectColumns: cols, whereClause: whereClause, arguments: leftSql.arguments + rightSql.arguments), rightCompiler)
	}
	func add(argument: SqlValue) -> String {
		let arg = SqlArgument(name: "\(argPrefix)\(arguments.count)", value: argument)
		self.arguments.append(arg)
		return "{\(arg.name)}"
	}
	
}

