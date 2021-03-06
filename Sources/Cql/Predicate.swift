//
//  PredicateBuilder.swift
//  Sql
//
//  Created by Neil Allain on 4/9/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

/**
A single part of a predicate that can be composed with other predicate parts to make a complete predicates
*/
protocol PredicatePart {
	associatedtype Model: Codable
	func evaluate(evaluator: PredicateEvaluator<Model>, _ model: Model) -> Bool
	func sql(compiler: SqlPredicateCompiler<Model>) -> String
}

/**
The operation joining a group of predicate parts
*/
enum PredicateComposition {
	case any
	case all
	
	var sql: String {
		switch self {
		case .any:
			return "or"
		case .all:
			return "and"
		}
	}
}


/**
A type-erased predicate
*/
public struct Predicate<Model: Codable>: PredicatePart {
	private let eval: (PredicateEvaluator<Model>, Model) -> Bool
	private let sqlFn: (SqlPredicateCompiler<Model>) -> String
	init<T: PredicatePart>(_ part: T) where T.Model == Model {
		eval = part.evaluate
		sqlFn = part.sql
	}
	func evaluate(evaluator: PredicateEvaluator<Model>, _ model: Model) -> Bool {
		return eval(evaluator, model)
	}
	func sql(compiler: SqlPredicateCompiler<Model>) -> String {
		return sqlFn(compiler)
	}
}

/**
An operator and value for predicate conditions
*/
enum PredicateValueOperator<V: SqlComparable> {
	case equal(V)
	case lessThan(V)
	case lessThanOrEqual(V)
	case greaterThan(V)
	case greaterThanOrEqual(V)
	case anyValue([V])
	case anyPredicate(AnySubPredicate<V>)

	static func any(_ v: [V]) -> PredicateValueOperator<V> {
		return .anyValue(v)
	}
	static func any(_ v: AnySubPredicate<V>) -> PredicateValueOperator<V> {
		return .anyPredicate(v)
	}
	
	func sql<T>(compiler: SqlPredicateCompiler<T>) -> String {
		switch self {
		case .equal(let v):
			return "= \(compiler.add(argument: v.sqlValue))"
		case .lessThan(let v):
			return "< \(compiler.add(argument: v.sqlValue))"
		case .lessThanOrEqual(let v):
			return "<= \(compiler.add(argument: v.sqlValue))"
		case .greaterThan(let v):
			return "> \(compiler.add(argument: v.sqlValue))"
		case .greaterThanOrEqual(let v):
			return ">= \(compiler.add(argument: v.sqlValue))"
		case .anyValue(let vs):
			let argNames = vs.map { compiler.add(argument: $0.sqlValue) }
			return "in (\(argNames.joined(separator: ", ")))"
		case .anyPredicate(let v):
			return "in (\(v.sql(compiler)))"
		}
	}
}

/**
A predicate that always evaluates as true
*/
struct TruePredicatePart<Model: Codable>: PredicatePart {
	init() {
	}
	func evaluate(evaluator: PredicateEvaluator<Model>, _ model: Model) -> Bool { return true }
	func sql(compiler: SqlPredicateCompiler<Model>) -> String {
		return ""
	}
}

/**
A logical 'and' or 'or' of 2 predicates
*/
struct ComposePredicate<Model: Codable>: PredicatePart {
	private let left: Predicate<Model>
	private let right: Predicate<Model>
	private let op: PredicateComposition
	init(_ op: PredicateComposition, _ left: Predicate<Model>, _ right: Predicate<Model>) {
		self.op = op
		self.left = left
		self.right = right
	}
	func evaluate(evaluator: PredicateEvaluator<Model>, _ model: Model) -> Bool {
		let l = left.evaluate(evaluator: evaluator, model)
		let r = right.evaluate(evaluator: evaluator, model)
		switch op {
		case .all:
			return l && r
		case .any:
			return l || r
		}
	}
	func sql(compiler: SqlPredicateCompiler<Model>) -> String {
		let l = left.sql(compiler: compiler)
		let r = right.sql(compiler: compiler)
		return "(\(l)) \(op.sql) (\(r))"
	}

}
/**
A component of a predicate comparing a key path to another value
*/
struct ComparePropertyValue<Model: Codable, V: SqlComparable>: PredicatePart {
	private let path: WritableKeyPath<Model,V>
	private let valueOperator: PredicateValueOperator<V>
	init(_ path: WritableKeyPath<Model, V>, _ val: PredicateValueOperator<V>) {
		self.path = path
		self.valueOperator = val
	}
	func evaluate(evaluator: PredicateEvaluator<Model>, _ model: Model) -> Bool {
		let p = model[keyPath: path]
		return evaluator.evaluate(p, valueOperator)
	}
	func sql(compiler: SqlPredicateCompiler<Model>) -> String {
		let propName = compiler.name(for: path)
		return "\(propName) \(valueOperator.sql(compiler: compiler))"
	}
}
struct CompareOptionalPropertyValue<Model: Codable, V: SqlComparable>: PredicatePart {
	private let path: WritableKeyPath<Model,V?>
	private let valueOperator: PredicateValueOperator<V>
	init(_ path: WritableKeyPath<Model, V?>, _ val: PredicateValueOperator<V>) {
		self.path = path
		self.valueOperator = val
	}
	func evaluate(evaluator: PredicateEvaluator<Model>, _ model: Model) -> Bool {
		guard let p = model[keyPath: path] else { return false }
		return evaluator.evaluate(p, valueOperator)
	}
	func sql(compiler: SqlPredicateCompiler<Model>) -> String {
		let propName = compiler.name(for: path)
		return "\(propName) \(valueOperator.sql(compiler: compiler))"
	}
}



struct SubPredicate<Property: SqlComparable, Model: Codable> {
	let selectProperty: WritableKeyPath<Model, Property>
	let predicate: Predicate<Model>

	func sql(compiler: SqlCompiler) -> String {
		let rightCompiler = compiler.childCompiler(for: Model.self)
		guard let rightTable = rightCompiler.table else {
			fatalError("table \(String(describing: Model.self)) not defined")
		}
		let predSql = rightCompiler.compile(predicate)
		let subSelect = "select \(rightCompiler.name(for: selectProperty)) from \(rightTable.name) as \(rightCompiler.tableAlias) where \(predSql.whereClause)"
		compiler.arguments.append(contentsOf: rightCompiler.arguments)
		return subSelect
	}
	func evaluate(evaluator: PredicateEvaluatorProtocol, value: Property) -> Bool {
		let childEval = evaluator.childEvaluator(Model.self)
		let matches = childEval.findAll(predicate)
		return matches.reduce(false) { $0 || $1[keyPath: self.selectProperty] == value }
	}
}

struct OptionalSubPredicate<Property: SqlComparable, Model: Codable> {
	let selectProperty: WritableKeyPath<Model, Property?>
	let predicate: Predicate<Model>
	
	func sql(compiler: SqlCompiler) -> String {
		let rightCompiler = compiler.childCompiler(for: Model.self)
		guard let rightTable = rightCompiler.table else {
			fatalError("table \(String(describing: Model.self)) not defined")
		}
		let predSql = rightCompiler.compile(predicate)
		let subSelect = "select \(rightCompiler.name(for: selectProperty)) from \(rightTable.name) as \(rightCompiler.tableAlias) where \(predSql.whereClause)"
		compiler.arguments.append(contentsOf: rightCompiler.arguments)
		return subSelect
	}
	func evaluate(evaluator: PredicateEvaluatorProtocol, value: Property) -> Bool {
		let childEval = evaluator.childEvaluator(Model.self)
		let matches = childEval.findAll(predicate)
		return matches.reduce(false) { $0 || $1[keyPath: self.selectProperty] == value }
	}

}

/**
A type-erased SubPredicate (the inner model is erased)
*/
public struct AnySubPredicate<Property: SqlComparable> {
	init<Model: Codable>(_ subPredicate: SubPredicate<Property, Model>) {
		self.sql = subPredicate.sql
		self.evaluate = subPredicate.evaluate
	}
	init<Model: Codable>(_ subPredicate: OptionalSubPredicate<Property, Model>) {
		self.sql = subPredicate.sql
		self.evaluate = subPredicate.evaluate
	}
	let sql: (SqlCompiler) -> String
	let evaluate: (PredicateEvaluatorProtocol, Property) -> Bool
}

public func <(_ lhs: UUID, _ rhs: UUID) -> Bool {
	return lhs.uuidString < rhs.uuidString
}

extension UUID: Comparable {
	
}

public func <(_ lhs: Bool, _ rhs: Bool) -> Bool {
	return !lhs && rhs
}

extension Bool: Comparable {
}

extension Sequence where Element == String {
	func sqlJoined(separator: String) -> String {
		return self.filter({!$0.isEmpty}).joined(separator: separator)
	}
	func sqlJoined(separator: String, _ map: (Element) -> Element) -> String {
		let nonEmpty = self.filter({!$0.isEmpty})
		return nonEmpty.count == 1 ? nonEmpty.first! : nonEmpty.map(map).joined(separator: separator)
	}
	func sqlJoinedGroups(separator: String) -> String {
		return self.sqlJoined(separator: separator) { return "(\($0))" }
	}
}
