//
//  File.swift
//  
//
//  Created by Neil Allain on 6/15/19.
//

import Foundation

public struct Order<T: Codable> {
	fileprivate init() {
		self.properties = []
	}
	/**
	Creates an order by for a single table query
	*/
	public init<P: SqlComparable>(by path: WritableKeyPath<T,P>, descending: Bool = false) {
		self.properties = [OrderByProperty(path, descending: descending)]
	}
	
	fileprivate init(properties: [OrderByProperty<T>]) {
		self.properties = properties
	}
	var isOrderd: Bool { !properties.isEmpty }
	
	public func then<P: SqlComparable>(by path: WritableKeyPath<T, P>, descending: Bool = false) -> Order<T> {
		let p = OrderByProperty(path, descending: descending)
		var props = properties
		props.append(p)
		return Order(properties: props)
	}
	func lessThan(_ lhs: T, _ rhs: T) -> Bool {
		for prop in properties {
			if prop.lessThan(lhs, rhs) { return true }
		}
		return false
	}
	func equalTo(_ lhs: T, _ rhs: T) -> Bool {
		return properties.reduce(true) { e, p in
			return e && p.equalTo(lhs, rhs)
		}
	}
	func sql(compiler: SqlPredicateCompiler<T>) -> String {
		if isOrderd {
			let sqlOrder = properties.map { $0.sqlFn(compiler) }
			return "order by " + sqlOrder.joined(separator: ", ")
		}
		return ""
	}
	private var properties: [OrderByProperty<T>] = []
}

//fileprivate protocol OrderPropertyProtocol {
//	associatedtype T: Codable
//	var lessThan: (T, T) -> Bool {get}
//	var equalTo: (T, T) -> Bool {get}
//	var sqlFn: (SqlPredicateCompiler<T>) -> String {get}
//}
fileprivate struct OrderByProperty<T: Codable> {
	let lessThan: (T, T) -> Bool
	let equalTo: (T, T) -> Bool
	let sqlFn: (SqlPredicateCompiler<T>) -> String
	init(lessThan: @escaping (T, T) -> Bool, equalTo: @escaping (T, T) -> Bool, sqlFn: @escaping (SqlPredicateCompiler<T>) -> String) {
		self.lessThan = lessThan
		self.equalTo = equalTo
		self.sqlFn = sqlFn
	}
	init<P: SqlComparable>(_ property: WritableKeyPath<T, P>, descending: Bool = false) {
		lessThan = { lhs, rhs in
			let c = lhs[keyPath: property] < rhs[keyPath: property]
			return descending ? !c : c
		}
		equalTo = { lhs, rhs in
			return  lhs[keyPath: property] == rhs[keyPath: property]
		}
		
		sqlFn = { compiler in
			let name = compiler.name(for: property)
			return name + (descending ? " desc" : " asc")
		}
	}

}

public extension Order where T: SqlJoin {
	/**
	Creates an order for a joined query
	- Parameter by the property of the joined table to sort by
	- Parameter through The property on the join that represents the joined table
	- Parameter descending If true the orders is descending
	*/
	init<J: Codable, P: SqlComparable>(by path: WritableKeyPath<J,P>, through join: WritableKeyPath<T, J>, descending: Bool = false) {
		self.init(properties: [OrderByProperty(join, path, descending: descending)])
	}

}
fileprivate extension OrderByProperty where T: SqlJoin {
	init<J: Codable, P: SqlComparable>(_ join: WritableKeyPath<T, J>, _ property: WritableKeyPath<J, P>, descending: Bool = false) {
		let fullPath = join.appending(path: property)
		let lessThan: (T, T) -> Bool = { lhs, rhs in
			let c = lhs[keyPath: fullPath] < rhs[keyPath: fullPath]
			return descending ? !c : c
		}
		let equalTo: (T, T) -> Bool = { lhs, rhs in
			return  lhs[keyPath: fullPath] == rhs[keyPath: fullPath]
		}
		
		let sqlFn: (SqlPredicateCompiler<T>) -> String = { compiler in
			let name = compiler.name(for: property, through: join)
			return name + (descending ? " desc" : " asc")
		}
		self.init(lessThan: lessThan, equalTo: equalTo, sqlFn: sqlFn)
	}
}
//fileprivate struct OrderByJoinedProperty<T: SqlJoin>: OrderPropertyProtocol {
//	let lessThan: (T, T) -> Bool
//	let equalTo: (T, T) -> Bool
//	let sqlFn: (SqlPredicateCompiler<T>) -> String
//
//}
