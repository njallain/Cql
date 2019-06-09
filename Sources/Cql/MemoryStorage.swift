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
	public func schema<T: Codable>(for tableType: T.Type) -> TableSchema<T>? {
		return nil
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

	public func delete<T: Codable>(_ predicate: Predicate<T>) throws {
		let eval = PredicateEvaluator<T>(storage: self.storage)
		let rows = storage.rows(T.self).filter { !predicate.evaluate(evaluator: eval, $0) }
		storage.set(rows: rows)
	}
	public func find<T: Codable>(_ predicate: Predicate<T>, pagedBy: Int, results: ([T]) -> Bool) throws {
		let eval = PredicateEvaluator<T>(storage: self.storage)
		var partialResults = [T]()
		for row in eval.findAll(predicate) {
			partialResults.append(row)
			if partialResults.count == pagedBy {
				if results(partialResults) { partialResults.removeAll() }
				else { return }
			}
		}
		if partialResults.count > 0 {
			_ = results(partialResults)
		}
	}
	
	public func find<T1: Codable, T2: Codable>(_ predicate: JoinedPredicate<T1, T2>, pagedBy: Int, results: ([(T1,T2)]) -> Bool) throws {
		let leftEval = PredicateEvaluator<T1>(storage: self.storage)
		let rightEval = PredicateEvaluator<T2>(storage: self.storage)
		let leftObjs = leftEval.findAll(predicate.leftPredicate)
		let rightObjs = rightEval.findAll(predicate.rightPredicate)
		
		var rows = [(T1, T2)]()
		for leftObj in leftObjs {
			let matches = rightObjs.filter { rightObj in
				predicate.joinExpressions.reduce(true, {$0 && $1.evaluate(leftObj, rightObj)})
			}
			for rightObj in matches {
				rows.append((leftObj, rightObj))
				if rows.count == pagedBy {
					if results(rows) { rows.removeAll() }
					else { return }
				}
			}
		}
		if rows.count > 0 {
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
