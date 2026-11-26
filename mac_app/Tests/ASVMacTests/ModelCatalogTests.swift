import Foundation
import XCTest
@testable import ASVChat

final class ModelCatalogTests: XCTestCase {
    func testVisibleModelsKeepServerOrderAndSupportedAPIEfforts() throws {
        let rows: [[String: Any]] = [
            ["slug": "hidden", "visibility": "hide"],
            ["slug": "z-model", "visibility": "list", "display_name": "Z model",
             "default_reasoning_level": "high",
             "supported_reasoning_levels": [["effort": "high"], ["effort": "ultra"], ["effort": "low"], ["effort": "high"]]],
            ["slug": "a-model", "visibility": "list", "display_name": "", "default_reasoning_level": "high"],
            ["slug": " ", "visibility": "list"],
            ["slug": "z-model", "visibility": "list", "display_name": "Duplicate"]
        ]
        let models = try ChatModel.catalog(from: ["models": rows])
        XCTAssertEqual(models.map(\.id), ["z-model", "a-model"])
        XCTAssertEqual(models.map(\.displayName), ["Z model", "a-model"])
        XCTAssertEqual(models[0].supportedEfforts, ["high", "low"])
        XCTAssertTrue(models[1].supportedEfforts.isEmpty)
        XCTAssertThrowsError(try ChatModel.catalog(from: ["models": []]))
        XCTAssertThrowsError(try ChatModel.catalog(from: ["data": rows]))
    }

    func testKnownModelFallbackIsOnlyUsedForMissingCapabilityField() throws {
        func levels(_ id: String, advertised: [String]? = nil) throws -> [String] {
            var row: [String: Any] = ["slug": id, "visibility": "list"]
            if let advertised { row["supported_reasoning_levels"] = advertised.map { ["effort": $0] } }
            return try XCTUnwrap(ChatModel.catalog(from: ["models": [row]]).first).supportedEfforts
        }
        XCTAssertEqual(try levels("gpt-6.2-sol"), ["low", "medium", "high", "xhigh", "max"])
        XCTAssertTrue(try levels("gpt-6.1-terra").contains("none"))
        XCTAssertFalse(try levels("gpt-5.5").contains("max"))
        XCTAssertEqual(try levels("gpt-6.2-sol", advertised: ["low"]), ["low"])
        XCTAssertEqual(try levels("gpt-6.2-sol", advertised: []), [])
        XCTAssertEqual(try levels("gpt-6.2-sol-other"), [])
    }

    func testResponseUsesSelectedSlugAndOnlyExplicitCompatibleEffort() throws {
        let model = ChatModel(id: "a-model", displayName: "A Model", supportedEfforts: ["none", "high"])
        let input = [["role": "user", "content": "質問"]]
        func body(effort: String?) throws -> [String: Any] {
            let request = try OpenAIConnection.answerRequest(token: "fixture", model: model, effort: effort, input: input)
            let data = try XCTUnwrap(request.httpBody)
            return try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: Any])
        }
        let selected = try body(effort: "high")
        XCTAssertEqual(selected["model"] as? String, "a-model")
        XCTAssertEqual(selected["reasoning"] as? [String: String], ["effort": "high"])
        XCTAssertEqual(selected["input"] as? [[String: String]], input)
        XCTAssertEqual(selected["stream"] as? Bool, true)
        XCTAssertEqual(selected["store"] as? Bool, false)
        XCTAssertEqual(try body(effort: "none")["reasoning"] as? [String: String], ["effort": "none"])
        XCTAssertNil(try body(effort: nil)["reasoning"])
        XCTAssertNil(try body(effort: "low")["reasoning"])
    }
}
