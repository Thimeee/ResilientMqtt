using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace ResilientMqtt.Tests.Infrastructure
{
    /// <summary>A throwaway PKI for TLS tests: a CA, a broker certificate and a client certificate.</summary>
    internal static class TestPki
    {
        public static X509Certificate2 CreateCa(string name = "CN=ResilientMqtt Test CA")
        {
            using var key = RSA.Create(2048);
            var req = new CertificateRequest(name, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            req.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            req.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
            req.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(req.PublicKey, false));
            return Persist(req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1)));
        }

        /// <summary>Broker certificate issued by <paramref name="ca"/>, with the private key (usable by SslStream).</summary>
        public static X509Certificate2 CreateServerCert(X509Certificate2 ca, bool withLoopbackNames = true, string subject = "CN=localhost")
        {
            using var key = RSA.Create(2048);
            var req = new CertificateRequest(subject, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            req.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
            req.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
            req.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new("1.3.6.1.5.5.7.3.1") }, false));   // serverAuth
            if (withLoopbackNames)
            {
                var san = new SubjectAlternativeNameBuilder();
                san.AddDnsName("localhost");
                san.AddIpAddress(IPAddress.Loopback);
                req.CertificateExtensions.Add(san.Build());
            }
            using var issued = req.Create(ca, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddMonths(6), RandomSerial());
            return Persist(issued.CopyWithPrivateKey(key));
        }

        /// <summary>Client certificate (clientAuth) issued by <paramref name="ca"/>, written to a password-protected PFX.</summary>
        public static (string PfxPath, string Password, string Thumbprint) CreateClientPfx(X509Certificate2 ca)
        {
            using var key = RSA.Create(2048);
            var req = new CertificateRequest("CN=kiosk-test-client", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            req.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
            req.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
            req.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new("1.3.6.1.5.5.7.3.2") }, false));   // clientAuth
            using var issued = req.Create(ca, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddMonths(6), RandomSerial());
            using var withKey = issued.CopyWithPrivateKey(key);

            const string password = "test-pfx-password";
            var path = Path.Combine(Path.GetTempPath(), $"rmqtt-client-{Guid.NewGuid():N}.pfx");
            File.WriteAllBytes(path, withKey.Export(X509ContentType.Pfx, password));
            return (path, password, withKey.Thumbprint);
        }

        public static string WritePem(X509Certificate2 cert)
        {
            var path = Path.Combine(Path.GetTempPath(), $"rmqtt-ca-{Guid.NewGuid():N}.pem");
            File.WriteAllText(path, cert.ExportCertificatePem());
            return path;
        }

        public static bool IssuedBy(X509Certificate presented, X509Certificate2 ca)
        {
            using var leaf = X509CertificateLoader.LoadCertificate(presented.GetRawCertData());
            using var chain = new X509Chain();
            chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            chain.ChainPolicy.CustomTrustStore.Add(ca);
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            return chain.Build(leaf);
        }

        // Round-trip through PFX so Windows SChannel gets a key it can use (ephemeral keys are rejected by SslStream).
        private static X509Certificate2 Persist(X509Certificate2 cert)
            => X509CertificateLoader.LoadPkcs12(cert.Export(X509ContentType.Pfx), null, X509KeyStorageFlags.Exportable);

        private static byte[] RandomSerial() => RandomNumberGenerator.GetBytes(8);
    }
}
