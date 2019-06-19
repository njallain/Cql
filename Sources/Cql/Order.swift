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
	public init<P: SqlComparable>(by path: WritableKeyPath<T,P>, descending: Bool = false) {
		self.properties = [OrderByProperty(path, descending: descending)]
	}
	private init(properties: [OrderByProperty<T>]) {
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

fileprivate struct OrderByProperty<T: Codable> {
	fileprivate let lessThan: (T, T) -> Bool
	fileprivate let equalTo: (T, T) -> Bool
	fileprivate let sqlFn: (SqlPredicateCompiler<T>) -> String
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

public struct JoinedOrder<T: Codable, U: Codable> {
	public init(_ order: Order<T>, _ leftType: U.Type) {
		self.order = order
	}
	private let order: Order<T>
	func lessThan(_ lhs: (T,U), _ rhs: (T,U)) -> Bool {
		return order.lessThan(lhs.0, rhs.0)
	}
	func equalTo(_ lhs: (T,U), _ rhs: (T,U)) -> Bool {
		return order.equalTo(lhs.0, rhs.0)
	}
	func sql(compiler: (SqlPredicateCompiler<T>, SqlPredicateCompiler<U>)) -> String {
		return order.sql(compiler: compiler.0)
	}

}
