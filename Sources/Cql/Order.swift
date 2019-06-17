//
//  File.swift
//  
//
//  Created by Neil Allain on 6/15/19.
//

import Foundation

public enum Order {
	public static func by<T: Codable, P: SqlComparable>(_ path: WritableKeyPath<T, P>, descending: Bool = false) -> SingleOrder<T> {
		return SingleOrder<T>().then(by: path, descending: descending)
	}
}
public struct SingleOrder<T: Codable> {
	fileprivate init() {
		self.properties = []
	}
	private init(properties: [OrderByProperty<T>]) {
		self.properties = properties
	}
	var isOrderd: Bool { !properties.isEmpty }
	
	public func then<P: SqlComparable>(by path: WritableKeyPath<T, P>, descending: Bool = false) -> SingleOrder<T> {
		let p = OrderByProperty(path, descending: descending)
		var props = properties
		props.append(p)
		return SingleOrder(properties: props)
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

