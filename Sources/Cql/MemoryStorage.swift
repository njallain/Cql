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
	private var keyAllocators = [String: Any]()
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
	public func keyAllocator<T>(for type: T.Type) -> AnyKeyAllocator<T.Key> where T : PrimaryKeyTable {
		let tkey = String(describing: T.self)
		if let allocator = keyAllocators[tkey] {
			return allocator as! AnyKeyAllocator<T.Key>
		}
		let conn = try! self.open()
		let allocator = try! type.keyAllocator(conn)
		keyAllocators[tkey] = allocator
		return allocator
	}
}

public class MemoryConnection: StorageConnection {
	private weak var _storage: MemoryStorage?
	private var mem: MemoryStorage { _storage! }
	public var storage: Storage { self._storage! }
	init(storage: MemoryStorage) {
		self._storage = storage
	}
	
	public func beginTransaction() throws -> Transaction {
		return Transaction(
			commit: {  },
			rollback: {  }
		)
	}
	
	public func insert<T: Codable>(_ rows: [T]) throws {
		let existing = mem.rows(T.self)
		mem.set(rows: rows + existing)
	}
	
	public func update<T: Codable>(where predicate: Predicate<T>, set: (inout T) -> Void) throws {
		let eval = PredicateEvaluator<T>(storage: self.mem)
		let rows: [T] = mem.rows(T.self)
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
		mem.set(rows: newRows)
	}
	public func update<T: PrimaryKeyTable>(_ rows: [T]) throws {
		let updatesById = Dictionary(grouping: rows, by: {$0[keyPath: T.primaryKey]})
		let rows = mem.rows(T.self)
		var newRows = [T]()
		for row in rows {
			if let updated = updatesById[row[keyPath: T.primaryKey]]?.first {
				newRows.append(updated)
			} else {
				newRows.append(row)
			}
		}
		mem.set(rows: newRows)
	}
	public func update<T: PrimaryKeyTable2>(_ rows: [T]) throws {
		let updatesById = Dictionary(grouping: rows, by: {$0.primaryKeys})
		let rows = mem.rows(T.self)
		var newRows = [T]()
		for row in rows {
			if let updated = updatesById[row.primaryKeys]?.first {
				newRows.append(updated)
			} else {
				newRows.append(row)
			}
		}
		mem.set(rows: newRows)
	}

	public func delete<T: Codable>(_ predicate: Predicate<T>) throws {
		let eval = PredicateEvaluator<T>(storage: self.mem)
		let rows = mem.rows(T.self).filter { !predicate.evaluate(evaluator: eval, $0) }
		mem.set(rows: rows)
	}
	public func fetch<T: Codable>(query: Query<T>, results: ([T]) -> Bool) throws {
		let eval = PredicateEvaluator<T>(storage: self.mem)
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
	
	public func fetch<T: AnyJoin>(query: JoinedQuery<T>, results: ([T]) -> Bool) throws {
		let leftEval = PredicateEvaluator<T.Left>(storage: self.mem)
		let rightEval = PredicateEvaluator<T.Right>(storage: self.mem)
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
		let maxId = mem.rows(type).map({$0[keyPath: type.primaryKey]}).max() ?? 0
		return maxId + 1
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
