//
//  JoinedPredicate.swift
//  Sql
//
//  Created by Neil Allain on 5/11/19.
//  Copyright © 2019 Neil Allain. All rights reserved.
//

import Foundation

struct JoinExpression<LeftModel: Codable, RightModel: Codable, Property: SqlConvertible> {
	let left: WritableKeyPath<LeftModel, Property>
	let right: WritableKeyPath<RightModel, Property>
	
	func evaluate(_ lhs: LeftModel, _ rhs: RightModel) -> Bool {
		return lhs[keyPath: left].sqlValue == rhs[keyPath: right].sqlValue
	}
	func leftName(compiler: SqlPredicateCompiler<LeftModel>) -> String {
		return compiler.name(for: left)
	}
	func rightName(compiler: SqlPredicateCompiler<RightModel>) -> String {
		return compiler.name(for: right)
	}
}

struct AnyJoinExpression<LeftModel: Codable, RightModel: Codable> {
	init<Property: SqlConvertible>(_ joinExpression: JoinExpression<LeftModel, RightModel, Property>) {
		self.evaluate = joinExpression.evaluate
		self.leftName = joinExpression.leftName
		self.rightName = joinExpression.rightName
	}
	let evaluate: (LeftModel, RightModel) -> Bool
	let leftName: (SqlPredicateCompiler<LeftModel>) -> String
	let rightName: (SqlPredicateCompiler<RightModel>) -> String
}


struct JoinedPredicate<LeftModel: Codable, RightModel: Codable> {
	var joinExpressions: [AnyJoinExpression<LeftModel, RightModel>]
	let leftPredicate: Predicate<LeftModel>
	let rightPredicate: Predicate<RightModel>
	
	public mutating func on<Property: SqlConvertible>(_ leftPath: WritableKeyPath<LeftModel, Property>, equals rightPath: WritableKeyPath<RightModel, Property>) -> JoinedPredicate<LeftModel, RightModel> {
		self.joinExpressions.append(AnyJoinExpression(JoinExpression(left: leftPath, right: rightPath)))
		return self
	}
}

