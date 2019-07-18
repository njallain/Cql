//
//  MemoryStorage.swift
//  Sql
//
//  Created by Neil Allain on 5/12/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

public class MemoryStorage: Storage {
	private var allRows: [String: [Any]] = [:]
	fileprivate var nextIds: [String: Int] = [:]
	public init() {
		
	}
	public func schema<T: Codable>(for tableType: T.Type) -> TableSchema<T> {
		fatalError("MemoryStorage does not have schemas")
	}
	public func open() throws -> StorageConnection {
		return MemoryConnection(storage: self)
	}
	public func checkSchema() throws -> [SchemaDifference] {
		return []
	}
	public func delete() throws {
	}

	func rows<T: Codable>(_ type: T.Type) -> [T] {
		let key = String(describing: type)
		if let rows = allRows[key] {
			return rows as! [T]
		}
		return []
	}
	func set<T: Codable>(rows: [T]) {
		let key = String(describing: T.self)
		allRows[key] = rows
	}
}

public class MemoryConnection: StorageConnection {
	private let storage: MemoryStorage
	init(storage: MemoryStorage) {
		self.storage = storage
	}
	
	public func beginTransaction() throws -> Transaction {
		return Transaction(
			commit: {  },
			rollback: {  }
		)
	}
	
	public func insert<T: Codable>(_ rows: [T]) throws {
		let existing = storage.rows(T.self)
		storage.set(rows: rows + existing)
	}
	
	public func update<T: Codable>(where predicate: Predicate<T>, set: (inout T) -> Void) throws {
		let eval = PredicateEvaluator<T>(storage: self.storage)
		let rows: [T] = storage.rows(T.self)
//		let newRows = rows.map({ row in
//			if predicate.evaluate(evaluator: eval, row) {
//				var v = row
//				set(&v)
//				return v
//			}
//			return row
//		})
		var newRows = [T]()
		for row in rows {
			if predicate.evaluate(evaluator: eval, row) {
				var v = row
				set(&v)
				newRows.append(v)
			} else {
				newRows.append(row)
			}
		}
		storage.set(rows: newRows)
	}
	public func update<T: PrimaryKeyTable>(_ rows: [T]) throws {
		let updatesById = Dictionary(grouping: rows, by: {$0[keyPath: T.primaryKey]})
		let rows = storage.rows(T.self)
		var newRows = [T]()
		for row in rows {
			if let updated = updatesById[row[keyPath: T.primaryKey]]?.first {
				newRows.append(updated)
			} else {
				newRows.append(row)
			}
		}
		storage.set(rows: newRows)
	}
	public func update<T: PrimaryKeyTable2>(_ rows: [T]) throws {
		let updatesById = Dictionary(grouping: rows, by: {$0.primaryKeys})
		let rows = storage.rows(T.self)
		var newRows = [T]()
		for row in rows {
			if let updated = updatesById[row.primaryKeys]?.first {
				newRows.append(updated)
			} else {
				newRows.append(row)
			}
		}
		storage.set(rows: newRows)
	}

	public func delete<T: Codable>(_ predicate: Predicate<T>) throws {
		let eval = PredicateEvaluator<T>(storage: self.storage)
		let rows = storage.rows(T.self).filter { !predicate.evaluate(evaluator: eval, $0) }
		storage.set(rows: rows)
	}
	public func fetch<T: Codable>(query: Query<T>, results: ([T]) -> Bool) throws {
		let eval = PredicateEvaluator<T>(storage: self.storage)
		var partialResults = [T]()
		var all = eval.findAll(query.predicate)
		if let order = query.order {
			all.sort(by: order.lessThan)
		}
		for row in all {
			partialResults.append(row)
			if partialResults.count == query.pageSize {
				if results(partialResults) { partialResults.removeAll() }
				else { return }
			}
		}
		if !partialResults.isEmpty {
			_ = results(partialResults)
		}
	}
	
	public func fetch<T: SqlJoin>(query: JoinedQuery<T>, results: ([T]) -> Bool) throws {
		let leftEval = PredicateEvaluator<T.Left>(storage: self.storage)
		let rightEval = PredicateEvaluator<T.Right>(storage: self.storage)
		let leftObjs = leftEval.findAll(query.predicate.leftPredicate)
		let rightObjs = rightEval.findAll(query.predicate.rightPredicate)
		
		var rows = [T]()
		for leftObj in leftObjs {
			let matches = rightObjs.filter { rightObj in
				query.predicate.joinExpressions.reduce(true, {$0 && $1.evaluate(leftObj, rightObj)})
			}
			rows.append(contentsOf: matches.map({
				var r = T()
				r[keyPath: T.left] = leftObj
				r[keyPath: T.right] = $0
				return r
			}))
		}
		if let order = query.order {
			rows.sort(by: order.lessThan)
		}
		var resultRows = [T]()
		for row in rows {
			resultRows.append(row)
			if resultRows.count == query.pageSize {
				if results(rows) { rows.removeAll() }
				else { return }
			}
		}
		if !rows.isEmpty {
			_ = results(rows)
		}
	}
	
	public func nextId<T>(_ type: T.Type) throws -> Int where T : PrimaryKeyTable, T.Key == Int {
		let key = String(describing: type)
		if let id = storage.nextIds[key] {
			storage.nextIds[key] = id + 1
			return id
		} else {
			let maxId = storage.rows(type).map({$0[keyPath: type.primaryKey]}).max() ?? 0
			storage.nextIds[key] = maxId + 2
			return maxId + 1
		}
		//return try self.database.nextId(type, connection: self)
	}
}

public protocol PredicateEvaluatorProtocol {
	func childEvaluator<U: Codable>(_ type: U.Type) -> PredicateEvaluator<U>
}
public class PredicateEvaluator<T: Codable>: PredicateEvaluatorProtocol {
	let storage: MemoryStorage
	
	init(storage: MemoryStorage) {
		self.storage = storage
	}

	public func childEvaluator<U: Codable>(_ type: U.Type) -> PredicateEvaluator<U> {
		return PredicateEvaluator<U>(storage: storage)
	}
	func findAll(_ predicate: Predicate<T>) -> [T] {
		return storage.rows(T.self).filter { predicate.evaluate(evaluator: self, $0) }
	}
	
	
	func evaluate<V: SqlComparable>(_ value: V, _ valueOperator: PredicateValueOperator<V>) -> Bool {
		switch valueOperator {
		case .equal(let v):
			return value == v
		case .lessThan(let v):
			return value < v
		case .lessThanOrEqual(let v):
			return value <= v
		case .greaterThan(let v):
			return value > v
		case .greaterThanOrEqual(let v):
			return value >= v
		case .anyValue(let vs):
			return vs.contains(value)
		case .anyPredicate(let subPredicate):
			return subPredicate.evaluate(self, value)
		}
	}
}
