import Foundation
import XCTest
@testable import ASVChat

final class ChatProtocolTests: XCTestCase {
    func testDynamicRegistrationUsesPKCEAndSavedClientOmitsRegistrationHint() throws {
        let redirect = URL(string: "http://127.0.0.1:12345/auth/callback")!
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
        func parameters(client: String?) throws -> [String: String] {
            let url = try OpenAIConnection.authorizationURL(client: client, host: "urn:uuid:test", redirect: redirect,
                                                            state: "test", nonce: "test", verifier: verifier,
                                                            hint: nil, consent: false)
            let items = URLComponents(url: url, resolvingAgainstBaseURL: false)!.queryItems!
            return Dictionary(uniqueKeysWithValues: items.map { ($0.name, $0.value!) })
        }
        let initial = try parameters(client: nil)
        XCTAssertEqual(initial["client_id"], "dynamic_agent_client")
        XCTAssertEqual(initial["redirect_uri"], redirect.absoluteString)
        XCTAssertEqual(initial["agent_name_hint"], "Ahaki Study Viewer")
        XCTAssertEqual(initial["code_challenge"], "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM")
        let saved = try parameters(client: "existing")
        XCTAssertEqual(saved["client_id"], "existing")
        XCTAssertNil(saved["agent_name_hint"])
    }

    func testIdentityOnlyDoesNotEnableInference() throws {
        let identity = try OpenAIConnection.tokens(from: ["id_token": "fixture", "scope": "openid profile"])
        XCTAssertFalse(identity.scopes.contains(OpenAIConnection.sharingScope))
        XCTAssertThrowsError(try OpenAIConnection.tokens(from: ["id_token": "fixture", "scope": OpenAIConnection.sharingScope]))
    }

    func testStreamRequiresCompletionAndHandlesLimitsAfterText() throws {
        var stream = AnswerStream()
        XCTAssertNil(try stream.consume(#"data: {"type":"response.output_text.delta","delta":"回答"}"#))
        XCTAssertThrowsError(try stream.consume(#"data: {"type":"response.failed","response":{"error":{"code":"subscription_sharing_usage_limit_exceeded"}}}"#)) {
            XCTAssertTrue(($0 as? ChatError)?.isUsageLimit == true)
        }
        var complete = AnswerStream()
        XCTAssertNil(try complete.consume(#"data: {"type":"response.output_text.delta","delta":"正解です。"}"#))
        XCTAssertEqual(try complete.consume(#"data: {"type":"response.completed"}"#), "正解です。")
        XCTAssertThrowsError(try stream.consume("data: [DONE]"))
    }

    func testOpenIDSignatureNonceAndAudienceAreChecked() throws {
        // Public key and matching token are synthetic test vectors; no private key is retained.
        let token = "eyJhbGciOiJSUzI1NiIsImtpZCI6InRlc3QifQ.eyJpc3MiOiJodHRwczovL2F1dGgub3BlbmFpLmNvbSIsImF1ZCI6WyJhc3YtdGVzdCJdLCJzdWIiOiJleGFtcGxlIiwibm9uY2UiOiJub25jZSIsImlhdCI6MTAwMCwiZXhwIjoxMjAwfQ.uEAIKoTBj8aVe2k01UrkgbBt1mfPFP4SkbQyq_MCeYQfagpkPrV83Qst1TsVSvfYD5ct29MWfuCx4nnhXRaDI2R7rwUaNhaHf92-yiK3eDatQfTpIBWviOJMfbBl99QlWbRd00acx8pp1Mxax9_ZDMBaZ0GLF2wh7ypOpBhUrGpWQwwOkqs_hao2Nsrb1_PsgMdtWvuBLDe6DiljBYyKr8ajFJLYVlpw44gkSyEsuJO3i9zJ6n6oS4ivdBlybdqiZ-rTDly9Qor2UoDR2yymZd22Eufq17afMzX8oJC1IIb9FjGGPUXFQvmQehs2tQUiOOHWivVe_QJrosMtZJh2Lw"
        let keys = [["kid": "test", "kty": "RSA", "n": "wCNj9Ze2gDjPtGC_TCUGXVuiKyvmKLYry3VEe8nx47tZgHMy04KZdHbDSwAkA8vzQSWws37xTy-WKImZPuhJZyYSFy-ezD9nB53VfqILgQgqxJUaN_3sd6S0Tfk4nRNbhhpIqIajtzjEiL05Fnpg-DKbRy0wsd2ziIynAY6mAelzHZTCBTqI7ztSPk2k_CnsODS5NdqDPIP5snL8HaDr0z63QCWfkcfkMEps9JIM1EV44nh2e8Pv_mTBnSDMKrMlUrfYKDKcR3ncpM8CKrhqS7lVwe_zcUt40uZzZldu-DoWxU2P5kkMGoSGQzCyZt1E3wpLrWgK8i6-qNwtXpvRjQ", "e": "AQAB"]]
        let time = Date(timeIntervalSince1970: 1100)
        func validate(_ value: String = token, client: String = "asv-test", nonce: String = "nonce") throws -> VerifiedIdentity {
            try VerifiedIdentity.verify(token: value, clientID: client, nonce: nonce, keys: keys, now: time)
        }
        XCTAssertEqual(try validate().subject, "example")
        XCTAssertThrowsError(try validate(client: "other"))
        XCTAssertThrowsError(try validate(nonce: "other"))
        let parts = token.components(separatedBy: ".")
        let modified = parts[0] + "." + parts[1] + ".x" + parts[2].dropFirst()
        XCTAssertThrowsError(try validate(modified))
        XCTAssertThrowsError(try VerifiedIdentity.verify(token: token, clientID: "asv-test", nonce: "nonce", keys: keys,
                                                        now: Date(timeIntervalSince1970: 1500)))
    }
}
