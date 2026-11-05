using System;
using System.IO;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using MQTTnet;

namespace ResilientMqtt.Internal
{
    /// <summary>
    /// Applies <see cref="TlsOptions"/> to the MQTTnet TLS builder (#37): server-certificate checking by mode
    /// (None / Normal / Hard / Ca) plus an optional client certificate for mutual TLS. Configuration errors
    /// (missing file, wrong password, no private key, unknown mode) throw at startup — fail fast, never at 2 a.m.
    /// </summary>
    internal enum TlsMode { None, Normal, Hard, Ca }

    internal static class TlsConfigurator
    {
        /// <summary>The one TLS-mode parser (R10 — shared with the validator). Empty → None; unknown → null.</summary>
        public static TlsMode? ParseMode(string? value)
            => (value ?? string.Empty).Trim().ToUpperInvariant() switch
            {
                "" or "NONE" => TlsMode.None,
                "NORMAL" => TlsMode.Normal,
                "HARD" => TlsMode.Hard,
                "CA" => TlsMode.Ca,
                _ => null,
            };

        public static void Configure(MqttClientTlsOptionsBuilder tls, TlsOptions options, ILogger logger)
        {
            var parsed = ParseMode(options.Mode);
            if (parsed is null)
            {
                // R6 — 1.0.3 treated an unknown mode as OS validation; keep that (no startup crash after an upgrade),
                // but say so. ValidateOnStart() reports it as an error.
                logger.LogWarning(ResilientMqttLogEvents.TlsInsecure,
                    "[ResilientMqttClient] Unknown Tls.Mode '{Mode}' — using OS certificate validation (\"None\"). Valid: None, Normal, Hard, Ca.",
                    options.Mode);
                parsed = TlsMode.None;
            }

            switch (parsed.Value)
            {
                case TlsMode.None:
                    break;   // OS validation (public CA / domain PKI)

                case TlsMode.Normal:
                    // #11 — accepts ANY server certificate (MITM-open). Fine for dev/internal LAN,
                    // NOT for production. Warn loudly so an insecure prod config isn't silent.
                    logger.LogWarning(ResilientMqttLogEvents.TlsInsecure,
                        "[ResilientMqttClient] TLS Mode='Normal' accepts ANY broker certificate " +
                        "(no MITM protection). Use Mode='Hard' (pinned thumbprint) or 'Ca' (private CA) in production.");
                    tls.WithCertificateValidationHandler(_ => true);
                    break;

                case TlsMode.Hard:
                    var pinned = NormalizeThumbprint(options.TrustedThumbprint);
                    tls.WithCertificateValidationHandler(ctx =>
                        ctx.Certificate is not null &&
                        NormalizeThumbprint(ctx.Certificate.GetCertHashString()) == pinned);
                    break;

                case TlsMode.Ca:
                    var ca = LoadCaCertificate(options.CaCertificatePath);
                    var checkName = options.CheckCertificateName;
                    tls.WithCertificateValidationHandler(ctx => ValidateAgainstCa(ctx.Certificate, ctx.Chain, ctx.SslPolicyErrors, ca, checkName));
                    break;
            }

            var clientCert = LoadClientCertificate(options);
            if (clientCert is not null)
                tls.WithClientCertificates(new[] { clientCert });
        }

        internal static bool ValidateAgainstCa(
            X509Certificate? serverCertificate, X509Chain? presentedChain, SslPolicyErrors errors, X509Certificate2 ca, bool checkName)
        {
            if (serverCertificate is null) return false;
            if (errors.HasFlag(SslPolicyErrors.RemoteCertificateNotAvailable)) return false;
            if (checkName && errors.HasFlag(SslPolicyErrors.RemoteCertificateNameMismatch)) return false;

            using var leaf = X509CertificateLoader.LoadCertificate(serverCertificate.GetRawCertData());
            using var chain = new X509Chain();
            chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            chain.ChainPolicy.CustomTrustStore.Add(ca);
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;   // private CAs rarely publish CRLs
            if (presentedChain is not null)   // intermediates the broker sent
                foreach (var element in presentedChain.ChainElements) chain.ChainPolicy.ExtraStore.Add(element.Certificate);
            return chain.Build(leaf);
        }

        private static X509Certificate2 LoadCaCertificate(string path)
        {
            if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
                throw new InvalidOperationException($"ResilientMqtt: Tls.Mode='Ca' needs Tls.CaCertificatePath (file not found: '{path}').");
            try
            {
                var text = File.ReadAllText(path);
                return text.Contains("-----BEGIN CERTIFICATE-----", StringComparison.Ordinal)
                    ? X509Certificate2.CreateFromPem(text)
                    : X509CertificateLoader.LoadCertificateFromFile(path);
            }
            catch (Exception ex) when (ex is not InvalidOperationException)
            {
                throw new InvalidOperationException($"ResilientMqtt: could not read the CA certificate '{path}': {ex.Message}", ex);
            }
        }

        private static X509Certificate2? LoadClientCertificate(TlsOptions o)
        {
            if (!string.IsNullOrWhiteSpace(o.ClientCertificatePath))
            {
                if (!File.Exists(o.ClientCertificatePath))
                    throw new InvalidOperationException($"ResilientMqtt: Tls.ClientCertificatePath not found: '{o.ClientCertificatePath}'.");
                X509Certificate2 cert;
                try
                {
                    cert = X509CertificateLoader.LoadPkcs12FromFile(o.ClientCertificatePath,
                        string.IsNullOrEmpty(o.ClientCertificatePassword) ? null : o.ClientCertificatePassword);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(
                        $"ResilientMqtt: could not open the client certificate '{o.ClientCertificatePath}' (wrong password or not a PFX?): {ex.Message}", ex);
                }
                return RequirePrivateKey(cert, o.ClientCertificatePath);
            }

            if (!string.IsNullOrWhiteSpace(o.ClientCertificateThumbprint))
            {
                var location = string.Equals(o.ClientCertificateStore, "CurrentUser", StringComparison.OrdinalIgnoreCase)
                    ? StoreLocation.CurrentUser : StoreLocation.LocalMachine;
                using var store = new X509Store(StoreName.My, location);
                store.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly);
                var found = store.Certificates.Find(X509FindType.FindByThumbprint, NormalizeThumbprint(o.ClientCertificateThumbprint), validOnly: false);
                if (found.Count == 0)
                    throw new InvalidOperationException(
                        $"ResilientMqtt: client certificate {o.ClientCertificateThumbprint} not found in {location}\\My.");
                return RequirePrivateKey(found[0], $"{location}\\My\\{o.ClientCertificateThumbprint}");
            }

            return null;
        }

        private static X509Certificate2 RequirePrivateKey(X509Certificate2 cert, string where)
            => cert.HasPrivateKey ? cert
               : throw new InvalidOperationException($"ResilientMqtt: the client certificate '{where}' has no private key (mutual TLS needs it).");

        internal static string NormalizeThumbprint(string? thumbprint)
            => new string((thumbprint ?? string.Empty).Where(Uri.IsHexDigit).ToArray()).ToUpperInvariant();
    }
}
