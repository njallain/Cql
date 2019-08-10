//
//  SqlPredicateBuilder.swift
//  Sql
//
//  Created by Neil Allain on 4/13/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

struct CompiledSql {
	let fullSql: String
	let selectColumns: [String]
	let whereClause: String
	let orderClause: String
	let arguments: [SqlArgument]
}
protocol SqlCompiler: AnyObject {
	var arguments: [SqlArgument] {get set}
	func childCompiler<C: Codable>(for type: C.Type) -> SqlPredicateCompiler<C>
}
class SqlPredicateCompiler<T: Codable>: SqlCompiler {
	let database: Database
	var tableAlias: String
	var tablePrefix: String { return tableAlias.isEmpty ? "" : tableAlias + "." }
	var argPrefix: String { return "arg\(tableAlias)" }
	let table: TableSchema<T>?
	var arguments = [SqlArgument]()
	
	init(database: Database, alias: String = "") {
		self.database = database
		self.tableAlias = alias
		self.table = database.schemaIfDefined(for: T.self)
	}
	
	func childCompiler<C: Codable>(for type: C.Type) -> SqlPredicateCompiler<C> {
		let name = database.schema(for: type).name
		return SqlPredicateCompiler<C>(database: self.database, alias: name)
	}
	func name<V: SqlConvertible>(for keyPath: WritableKeyPath<T,V>) -> String {
		guard let n = table?.column(keyPath: keyPath) else {
			fatalError("could not find column for key path: \(keyPath), this may be caused by attempting to query on a property that isn't SqlConvertible")
			//throw DatabaseError("could not find column for key path: \(keyPath)")
		}
		return "\(tablePrefix)\(n.name)"
	}
	func name<V: SqlConvertible>(for keyPath: WritableKeyPath<T,V?>) -> String {
		guard let n = table?.column(keyPath: keyPath) else {
			fatalError("could not find column for key path: \(keyPath), this may be caused by attempting to query on a property that isn't SqlConvertible")
			//throw DatabaseError("could not find column for key path: \(keyPath)")
		}
		return "\(tablePrefix)\(n.name)"
	}
	func compile(_ predicate: Predicate<T>) -> CompiledSql {
		return compile(Query(predicate: predicate))
	}
	func compile(_ query: Query<T>) -> CompiledSql {
		guard let table = table else {
			fatalError("table for \(String(describing: T.self)) not defined")
		}
		let colNames = table.columns.map({ "\(tablePrefix)\($0.name)" })
		let colSql = colNames.joined(separator: ", ")
		let whereClause = query.predicate.sql(compiler: self)
		let whereSql = whereClause.isEmpty ? "" : "where " + whereClause
		let orderSql = query.order?.sql(compiler: self) ?? ""
		let tableSql = tableAlias.isEmpty ? table.name : "\(table.name) as \(tableAlias)"
		let fullSql = "select \(colSql) from \(tableSql) \(whereSql) \(orderSql)"
		return CompiledSql(fullSql: fullSql, selectColumns: colNames, whereClause: whereClause, orderClause: orderSql, arguments: self.arguments)
	}
	func add(argument: SqlValue) -> String {
		let arg = SqlArgument(name: "\(argPrefix)\(arguments.count)", value: argument)
		self.arguments.append(arg)
		return "{\(arg.name)}"
	}
	
}

extension SqlPredicateCompiler where T: AnyJoin {
	func compile(_ query: JoinedQuery<T>) -> CompiledSql {
		/* to determine the alias names (the names of the left and right properties):
				- create an instance of the joined obj (using schema)
				- use relationship prop to change a value on the object - use that for diff detection
		*/
		let leftTable = database.schema(for: T.Left.self)
		let rightTable = database.schema(for: T.Right.self)
		let leftName = T.leftName(leftTable.newRow())
		let rightName = T.rightName(rightTable.newRow())
		let leftCompiler = SqlPredicateCompiler<T.Left>(database: database, alias: leftName)
		let leftSql = leftCompiler.compile(query.predicate.leftPredicate)
		let rightCompiler = SqlPredicateCompiler<T.Right>(database: database, alias: rightName)
		let rightSql = rightCompiler.compile(query.predicate.rightPredicate)
		let joins = query.predicate.joinExpressions.map {
			"\($0.leftName(leftCompiler)) = \($0.rightName(rightCompiler))"
		}
		let whereClause = [leftSql.whereClause, rightSql.whereClause].sqlJoinedGroups(separator: " and ")
		let whereSql = whereClause.isEmpty ? "" : " where " + whereClause
		let joinSql = joins.sqlJoinedGroups(separator: " and ")
		let cols = leftSql.selectColumns + rightSql.selectColumns
		let colSql = cols.joined(separator: ", ")
		let orderSql = query.order?.sql(compiler: self) ?? ""
		let fullSql = "select \(colSql) from \(leftTable.name) as \(leftCompiler.tableAlias) join \(rightTable.name) as \(rightCompiler.tableAlias) on \(joinSql)\(whereSql) \(orderSql)"
		return CompiledSql(fullSql: fullSql, selectColumns: cols, whereClause: whereClause, orderClause: orderSql, arguments: leftSql.arguments + rightSql.arguments)
	}

	func name<J: Codable, V: SqlConvertible>(for keyPath: WritableKeyPath<J,V>, through join: WritableKeyPath<T, J>) -> String {
		return name(for: keyPath, joinLeft: T.self, joinRight: J.self)
	}
	func name<J: Codable, V: SqlConvertible>(for keyPath: WritableKeyPath<J,V>, through join: WritableKeyPath<T, J?>) -> String {
		return name(for: keyPath, joinLeft: T.self, joinRight: J.self)
	}


	private func name<J: Codable, V: SqlConvertible>(for keyPath: WritableKeyPath<J,V>, joinLeft: T.Type, joinRight: J.Type) -> String {
		
		let joinedTable = database.schema(for: J.self)
		let joinName: String
		if J.self == T.Left.self {
			let table = database.schema(for: T.Left.self)
			joinName = T.leftName(table.newRow())
		} else {
			let table = database.schema(for: T.Right.self)
			joinName = T.rightName(table.newRow())
		}
		guard let column = joinedTable.column(keyPath: keyPath) else {
			fatalError("no column definied for \(String(describing: keyPath))")
		}
		return "\(joinName).\(column.name)"
	}
}
