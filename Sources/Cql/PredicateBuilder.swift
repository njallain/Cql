//
//  File.swift
//  
//
//  Created by Neil Allain on 7/13/19.
//

import Foundation

infix operator %==: ComparisonPrecedence
infix operator %>: ComparisonPrecedence
infix operator %<: ComparisonPrecedence
infix operator %>=: ComparisonPrecedence
infix operator %<=: ComparisonPrecedence
infix operator %&&: LogicalConjunctionPrecedence
infix operator %||: LogicalDisjunctionPrecedence
infix operator %*: ComparisonPrecedence	// sql 'in'


public extension WritableKeyPath where Root: Codable, Value: SqlComparable {
	static func %== (left: WritableKeyPath<Root, Value>, right: Value) -> Predicate<Root> {
		return Predicate(ComparePropertyValue(left, .equal(right)))
	}
	static func %== (left: WritableKeyPath<Root, Value?>, right: Value) -> Predicate<Root> {
		return Predicate(CompareOptionalPropertyValue(left, .equal(right)))
	}
	static func %>= (left: WritableKeyPath<Root, Value>, right: Value) -> Predicate<Root> {
		return Predicate(ComparePropertyValue(left, .greaterThanOrEqual(right)))
	}
	static func %> (left: WritableKeyPath<Root, Value>, right: Value) -> Predicate<Root> {
		return Predicate(ComparePropertyValue(left, .greaterThan(right)))
	}
	static func %<= (left: WritableKeyPath<Root, Value>, right: Value) -> Predicate<Root> {
		return Predicate(ComparePropertyValue(left, .lessThanOrEqual(right)))
	}
	static func %< (left: WritableKeyPath<Root, Value>, right: Value) -> Predicate<Root> {
		return Predicate(ComparePropertyValue(left, .lessThan(right)))
	}
	static func %* (left: WritableKeyPath<Root, Value>, right: [Value]) -> Predicate<Root> {
		return Predicate(ComparePropertyValue(left, .anyValue(right)))
	}
}

public extension Predicate {
	static func %&& (left: Predicate<Model>, right: Predicate<Model>) -> Predicate<Model> {
		return Predicate(ComposePredicate(.all, left, right))
	}
	static func %|| (left: Predicate<Model>, right: Predicate<Model>) -> Predicate<Model> {
		return Predicate(ComposePredicate(.any, left, right))
	}
	static func all(_ type: Model.Type) -> Self {
		return Predicate(TruePredicatePart<Model>())
	}
}

public extension RelationToOptionalMany {
	func `in`(_ predicate: Predicate<Target>) -> Predicate<Source> {
		//let oldPredicate = Predicate.all(Target.self).append(predicate)
		let subPred = AnySubPredicate(OptionalSubPredicate(selectProperty: self.keyPath, predicate: predicate))
		let inPred = ComparePropertyValue(Source.primaryKey, .anyPredicate(subPred))
		return Predicate(inPred)
	}
}

public extension RelationToMany {
	func `in`(_ predicate: Predicate<Target>) -> Predicate<Source> {
		//let oldPredicate = Predicate.all(Target.self).append(predicate)
		let subPred = AnySubPredicate(SubPredicate(selectProperty: self.keyPath, predicate: predicate))
		let inPred = ComparePropertyValue(Source.primaryKey, .anyPredicate(subPred))
		return Predicate(inPred)
	}
}


public extension RelationToOne {
	func `in`(_ predicate: Predicate<Target>) -> Predicate<Source> {
		//let oldPredicate = Predicate.all(Target.self).append(predicate)
		let subPred = AnySubPredicate(SubPredicate(selectProperty: Target.primaryKey, predicate: predicate))
		let inPred = ComparePropertyValue(self.keyPath, .anyPredicate(subPred))
		return Predicate(inPred)
	}

}
//@_functionBuilder
//public struct PredicateBuilder<T: Codable> {
//	public static func buildBlock(_ components: PredicatePart...) -> Predicate<T>
//		where PredicatePart.Model == T {
//		let pred = Predicate.all(T.self)
//			for part in components {
//				pred.append(part)
//			}
//			return pred
//	}
//}
