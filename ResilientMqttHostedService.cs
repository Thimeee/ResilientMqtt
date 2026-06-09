using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ResilientMqtt
{
    /// <summary>
    /// Optional hosted service that connects on app start and disconnects on stop.
    /// Register with <c>builder.AddAutoStart()</c> or manually via
    /// <c>services.AddHostedService&lt;MqttHostedService&gt;()</c>.
    /// </summary>
    public sealed class ResilientMqttHostedService : IHostedService
    {
        private readonly IResilientMqttClient _client;
        private readonly ILogger<ResilientMqttHostedService> _logger;

        public ResilientMqttHostedService(IResilientMqttClient client, ILogger<ResilientMqttHostedService> logger)
        {
            _client = client;
            _logger = logger;
        }

        public Task StartAsync(CancellationToken ct)
        {
            // Fire-and-forget — broker may be down at boot, reconnect loop handles it
            _ = Task.Run(async () =>
            {
                try
                {
                    _logger.LogInformation("[ResilientMqttClient] Auto-start: initial connect attempt");
                    var ok = await _client.ConnectAsync(ct);
                    _logger.LogInformation("[ResilientMqttClient] Auto-start result: {Result}",
                        ok ? "Success" : "Failed (reconnect will retry)");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[ResilientMqttClient] Auto-start threw");
                }
            }, ct);

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken ct) => _client.DisconnectAsync(ct);
    }
}
