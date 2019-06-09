import XCTest

import CqlTests

var tests = [XCTestCaseEntry]()
tests += CqlTests.allTests()
XCTMain(tests)
