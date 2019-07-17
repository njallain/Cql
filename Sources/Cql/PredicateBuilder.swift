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


extension WritableKeyPath where Root: Codable, Value: SqlComparable {
	static func %== (left: WritableKeyPath<Root, Value>, right: Value) -> AnyPredicatePart<Root> {
		return AnyPredicatePart(ComparePropertyValue(left, .equal(right)))
	}
	static func %>= (left: WritableKeyPath<Root, Value>, right: Value) -> AnyPredicatePart<Root> {
		return AnyPredicatePart(ComparePropertyValue(left, .greaterThanOrEqual(right)))
	}
	static func %> (left: WritableKeyPath<Root, Value>, right: Value) -> AnyPredicatePart<Root> {
		return AnyPredicatePart(ComparePropertyValue(left, .greaterThan(right)))
	}
	static func %<= (left: WritableKeyPath<Root, Value>, right: Value) -> AnyPredicatePart<Root> {
		return AnyPredicatePart(ComparePropertyValue(left, .lessThanOrEqual(right)))
	}
	static func %< (left: WritableKeyPath<Root, Value>, right: Value) -> AnyPredicatePart<Root> {
		return AnyPredicatePart(ComparePropertyValue(left, .lessThan(right)))
	}
	static func %* (left: WritableKeyPath<Root, Value>, right: [Value]) -> AnyPredicatePart<Root> {
		return AnyPredicatePart(ComparePropertyValue(left, .anyValue(right)))
	}
}

extension AnyPredicatePart {
	static func %&& (left: AnyPredicatePart<Model>, right: AnyPredicatePart<Model>) -> AnyPredicatePart<Model> {
		return AnyPredicatePart(ComposePredicate(.all, left, right))
	}
	static func %|| (left: AnyPredicatePart<Model>, right: AnyPredicatePart<Model>) -> AnyPredicatePart<Model> {
		return AnyPredicatePart(ComposePredicate(.any, left, right))
	}
	static func all(_ type: Model.Type) -> Self {
		return AnyPredicatePart(TruePredicatePart<Model>())
	}
}

extension RelationToMany {
	func `in`(_ predicate: AnyPredicatePart<Target>) -> AnyPredicatePart<Source> {
		let oldPredicate = Predicate.all(Target.self).append(predicate)
		let subPred = AnySubPredicate(SubPredicate(selectProperty: self.keyPath, predicate: oldPredicate))
		let inPred = ComparePropertyValue(Source.primaryKey, .anyPredicate(subPred))
		return AnyPredicatePart(inPred)
	}
}

extension RelationToOne {
	func `in`(_ predicate: AnyPredicatePart<Target>) -> AnyPredicatePart<Source> {
		let oldPredicate = Predicate.all(Target.self).append(predicate)
		let subPred = AnySubPredicate(SubPredicate(selectProperty: Target.primaryKey, predicate: oldPredicate))
		let inPred = ComparePropertyValue(self.keyPath, .anyPredicate(subPred))
		return AnyPredicatePart(inPred)
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
