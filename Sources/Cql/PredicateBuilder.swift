//
//  File.swift
//  
//
//  Created by Neil Allain on 7/13/19.
//

import Foundation

infix operator %==: ComparisonPrecedence
infix operator %&&: AdditionPrecedence

extension WritableKeyPath where Root: Codable, Value: SqlComparable {
	static func %== (left: WritableKeyPath<Root, Value>, right: Value) -> AnyPredicatePart<Root> {
		return AnyPredicatePart(ComparePropertyValue(left, .equal(right)))
	}
}

extension AnyPredicatePart {
	static func %&& (left: AnyPredicatePart<Model>, right: AnyPredicatePart<Model>) -> AnyPredicatePart<Model> {
		return AnyPredicatePart(ComposePredicate(.all, left, right))
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
