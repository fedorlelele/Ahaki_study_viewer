import Foundation
import Security

struct VerifiedIdentity {
    let subject: String
    let email: String?

    static func fetch(token: String, clientID: String, nonce: String) async throws -> Self {
        let endpoint = URL(string: OpenAIConnection.issuer + "/.well-known/jwks.json")!
        let response = try await OpenAIConnection.json(URLRequest(url: endpoint))
        return try verify(token: token, clientID: clientID, nonce: nonce,
                          keys: response["keys"] as? [[String: Any]] ?? [])
    }

    static func verify(token: String, clientID: String, nonce: String,
                       keys: [[String: Any]], now: Date = .now) throws -> Self {
        let parts = token.split(separator: ".", omittingEmptySubsequences: false).map(String.init)
        guard parts.count == 3,
              let header = decode(parts[0]), header["alg"] as? String == "RS256",
              let keyID = header["kid"] as? String,
              let jwk = keys.first(where: { $0["kid"] as? String == keyID && $0["kty"] as? String == "RSA" }),
              jwk["alg"] == nil || jwk["alg"] as? String == "RS256",
              jwk["use"] == nil || jwk["use"] as? String == "sig",
              let rsa = publicKey(jwk), let signature = Data(base64URL: parts[2]),
              SecKeyVerifySignature(rsa, .rsaSignatureMessagePKCS1v15SHA256,
                                    Data((parts[0] + "." + parts[1]).utf8) as CFData, signature as CFData, nil),
              let claims = decode(parts[1]),
              claims["iss"] as? String == OpenAIConnection.issuer,
              claims["nonce"] as? String == nonce,
              let subject = claims["sub"] as? String, !subject.isEmpty,
              let issued = claims["iat"] as? Double, issued <= now.timeIntervalSince1970 + 5,
              let expiry = claims["exp"] as? Double, expiry > now.timeIntervalSince1970 - 5,
              (claims["nbf"] as? Double ?? 0) <= now.timeIntervalSince1970 + 5 else {
            throw ChatError.invalidLogin
        }
        let audiences = claims["aud"] as? [String] ?? [claims["aud"] as? String ?? ""]
        let authorizedParty = claims["azp"] as? String
        guard audiences.contains(clientID),
              authorizedParty == nil || authorizedParty == clientID,
              audiences.count == 1 || authorizedParty == clientID else { throw ChatError.invalidLogin }
        let email = claims["email_verified"] as? Bool == true ? claims["email"] as? String : nil
        return VerifiedIdentity(subject: subject, email: email)
    }

    private static func decode(_ base64: String) -> [String: Any]? {
        guard let data = Data(base64URL: base64) else { return nil }
        return (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
    }

    // Security.framework imports RSA public keys in PKCS #1 DER format.
    private static func publicKey(_ jwk: [String: Any]) -> SecKey? {
        guard let n = jwk["n"] as? String, let e = jwk["e"] as? String,
              let modulus = Data(base64URL: n), (256...1024).contains(modulus.count),
              let exponent = Data(base64URL: e), (1...8).contains(exponent.count) else { return nil }
        func element(tag: UInt8, content: Data) -> Data {
            var length = content.count
            var bytes = [UInt8]()
            repeat { bytes.insert(UInt8(length & 0xff), at: 0); length >>= 8 } while length != 0
            if content.count >= 128 { bytes.insert(0x80 | UInt8(bytes.count), at: 0) }
            return Data([tag] + bytes) + content
        }
        func integer(_ bytes: Data) -> Data {
            let unsigned = Data(bytes.drop(while: { $0 == 0 }))
            let content = (unsigned.first ?? 0) >= 0x80 ? Data([0]) + unsigned : unsigned
            return element(tag: 0x02, content: content)
        }
        let der = element(tag: 0x30, content: integer(modulus) + integer(exponent))
        let attributes = [kSecAttrKeyClass: kSecAttrKeyClassPublic, kSecAttrKeyType: kSecAttrKeyTypeRSA] as CFDictionary
        return SecKeyCreateWithData(der as CFData, attributes, nil)
    }
}
