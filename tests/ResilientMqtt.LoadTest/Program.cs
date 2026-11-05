using ResilientMqtt.LoadTest;
using ResilientMqtt.TestKit;

// ResilientMqtt load / soak harness — normally started by tests/run-all.ps1.
//
//   certs <outDir>                                                  generate the test PKI for the docker Mosquitto
//   storm --clients 500 --outage 30 --mode proxy|restart --out <dir> reconnect storm
//   soak  --clients 100 --hours 24 [--minutes N] --fault-every 10 --out <dir>
//
// Exit code 0 = PASS, 1 = FAIL, 2 = usage / environment error.

static string Arg(string[] a, string name, string fallback)
{
    var i = Array.IndexOf(a, "--" + name);
    return i >= 0 && i + 1 < a.Length ? a[i + 1] : fallback;
}

if (args.Length == 0)
{
    Console.WriteLine("usage: certs <outDir> | storm [...] | soak [...]  (see Program.cs)");
    return 2;
}

try
{
    switch (args[0])
    {
        case "certs":
            Certs.Generate(args.Length > 1 ? args[1] : "certs", ChaosEnvironment.ClientPfxPassword);
            return 0;

        case "storm":
        {
            var ok = await new StormScenario(
                clients: int.Parse(Arg(args, "clients", "500")),
                outageSeconds: int.Parse(Arg(args, "outage", "30")),
                mode: Arg(args, "mode", "proxy"),
                outDir: Arg(args, "out", "results")).RunAsync();
            return ok ? 0 : 1;
        }

        case "soak":
        {
            var minutes = int.Parse(Arg(args, "minutes", "0"));
            var duration = minutes > 0 ? TimeSpan.FromMinutes(minutes) : TimeSpan.FromHours(double.Parse(Arg(args, "hours", "24")));
            var ok = await new SoakScenario(
                clients: int.Parse(Arg(args, "clients", "100")),
                duration: duration,
                faultEvery: TimeSpan.FromMinutes(double.Parse(Arg(args, "fault-every", "10"))),
                outDir: Arg(args, "out", "results")).RunAsync();
            return ok ? 0 : 1;
        }

        default:
            Console.WriteLine("unknown command " + args[0]);
            return 2;
    }
}
catch (Exception ex)
{
    Console.WriteLine("ERROR: " + ex);
    return 2;
}
