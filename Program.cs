using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace LentitudCli;

internal static class Program
{
    private const int MaxNickBytes = 32;
    private const int MaxMessageBytes = 4096;
    private const int RateLimit = 10;

    private static readonly object ConsoleLock = new();
    private static readonly object LogLock = new();
    private static readonly Queue<DateTimeOffset> SentTimestamps = new();
    private static readonly BlockingCollection<string> Outgoing = new();
    private static readonly CancellationTokenSource Cts = new();
    private static readonly List<LogEntry> Log = new();
    private static readonly DateTimeOffset SessionStart = DateTimeOffset.Now;

    private static string _nick = "";
    private static string _server = "";
    private static bool _promptVisible;
    private static ClientWebSocket _ws = new();

    private static async Task<int> Main(string[] args)
    {
        Console.WriteLine("lentitud-cli v7");

        string server = "ws://localhost:8080/ws";
        string? nickArg = null;

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--nick" or "-n" when i + 1 < args.Length:
                    nickArg = args[++i];
                    break;
                case "--server" or "-s" when i + 1 < args.Length:
                    server = args[++i];
                    break;
                default:
                    PrintPlain("  -s, --server <url>   WebSocket-адрес (по умолчанию ws://localhost:8080/ws)");
                    PrintPlain("  -n, --nick <nick>    ник (1-32 символа: латиница, цифры, '_', '-')");
                    return 2;
            }
        }

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            Cts.Cancel();
        };

        _nick = nickArg ?? "";
        while (!IsValidNick(_nick, out string? why))
        {
            if (why is not null) PrintPlain("! " + why);
            Console.Write("Ник (1-32 символа: латиница, цифры, '_', '-'): ");
            _nick = (Console.ReadLine() ?? "").Trim();
            if (Cts.IsCancellationRequested) return 1;
        }

        try
        {
            await RunAsync(server);
            return 0;
        }
        catch (OperationCanceledException)
        {
            return 0;
        }
        catch (WebSocketException ex)
        {
            PrintPlain($"! Ошибка соединения: {ex.Message}");
            return 1;
        }
    }

    private static async Task RunAsync(string server)
    {
        _server = server;

        while (true)
        {
            _ws = new ClientWebSocket();
            await _ws.ConnectAsync(new Uri(server), Cts.Token);
            PrintPlain($"* подключено к {server}");

            await SendJsonAsync(new JoinMessage("join", _nick));

            string? error = await JoinHandshakeAsync();
            if (error is null) break;

            if (error.Contains("nickname", StringComparison.OrdinalIgnoreCase))
            {
                await CloseQuietlyAsync();
                while (true)
                {
                    Console.Write("Ник занят. Введите другой: ");
                    _nick = (Console.ReadLine() ?? "").Trim();
                    if (IsValidNick(_nick, out string? why)) break;
                    if (why is not null) PrintPlain("! " + why);
                }
                continue;
            }

            await CloseQuietlyAsync();
            PrintPlain($"! Сервер отказал: {error}");
            return;
        }

        PrintPlain($"* подключены как {_nick}. Пустая строка, :q или Ctrl+C — выход. /help — справка.");

        Task receive = ReceiveLoopAsync();
        Task input = Task.Run(InputLoop);
        Task send = SendLoopAsync();

        await send;

        await CloseQuietlyAsync();
        try { await receive.WaitAsync(TimeSpan.FromSeconds(2)); } catch { }
        Outgoing.CompleteAdding();
    }

    private static async Task<string?> JoinHandshakeAsync()
    {
        while (true)
        {
            ServerMessage? msg = await ReceiveJsonAsync();
            if (msg is null) return "connection closed during join";

            if (msg.Type == "welcome") return null;
            if (msg.Type == "error") return msg.Text ?? "unknown error";

            LogIncoming(msg);
            PrintAbove(FormatIncoming(msg), ColorFor(msg));
        }
    }

    private static async Task ReceiveLoopAsync()
    {
        var buffer = new byte[8192];
        try
        {
            while (!Cts.IsCancellationRequested && _ws.State == WebSocketState.Open)
            {
                using var ms = new MemoryStream();
                WebSocketReceiveResult result;
                do
                {
                    result = await _ws.ReceiveAsync(buffer, Cts.Token);
                    if (result.MessageType == WebSocketMessageType.Close) return;
                    ms.Write(buffer, 0, result.Count);
                }
                while (!result.EndOfMessage);

                ServerMessage? msg;
                try
                {
                    msg = JsonSerializer.Deserialize(ms.ToArray(), AppJsonContext.Default.ServerMessage);
                }
                catch (JsonException)
                {
                    continue;
                }
                if (msg is not null)
                {
                    LogIncoming(msg);
                    PrintAbove(FormatIncoming(msg), ColorFor(msg));
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
    }

    private static void InputLoop()
    {
        try
        {
            while (!Cts.IsCancellationRequested)
            {
                PrintPrompt();
                string? line = Console.ReadLine();
                if (line is null) { Cts.Cancel(); return; }
                if (line.Length == 0)
                {
                    PrintPlain("* выход");
                    Cts.Cancel();
                    return;
                }
                if (line.StartsWith('/') || line.StartsWith(':'))
                {
                    if (!HandleCommand(line)) return;
                    continue;
                }
                Outgoing.Add(line);
            }
        }
        catch (Exception ex)
        {
            if (!Cts.IsCancellationRequested)
                PrintPlain($"! ввод не работает: {ex.Message}");
        }
    }

    /// <returns>false — если команда завершает работу</returns>
    private static bool HandleCommand(string line)
    {
        string cmd = line.Split(' ', 2)[0].ToLowerInvariant();

        switch (cmd)
        {
            case "/leave":
            case "/exit":
            case "/quit":
            case ":q":
            case ":q!":
            case ":wq":
                if (cmd == ":wq") ExportLog();
                PrintPlain("* выход");
                Cts.Cancel();
                return false;

            case "/clear":
            case ":c":
            case ":clear":
                lock (ConsoleLock)
                {
                    try { Console.Clear(); } catch { }
                }
                PrintPlain("* экран очищен (журнал сохранён — экспорт включает всю сессию)");
                return true;

            case "/export":
            case ":e":
            case ":export":
                ExportLog();
                return true;

            case "/help":
            case ":h":
            case ":help":
                PrintPlain("* команды: /help (:h) — справка; /clear (:c) — очистить экран;");
                PrintPlain("* /export (:e) — журнал сессии в markdown; /leave, :q, :wq — выход");
                return true;

            default:
                PrintPlain($@"! неизвестная команда ""{cmd}"". /help — список команд");
                return true;
        }
    }

    private static void LogIncoming(ServerMessage msg)
    {
        lock (LogLock)
            Log.Add(new LogEntry(msg.Type == "message" ? "message" : "system", msg.Nick, msg.Text));
    }

    private static void ExportLog()
    {
        string path = $"chat-{SessionStart:yyyy-MM-dd_HHmmss}.md";
        var sb = new StringBuilder();
        sb.AppendLine("# lentitud — журнал сессии");
        sb.AppendLine();
        sb.AppendLine($"- Сервер: `{_server}`");
        sb.AppendLine($"- Ник: `{_nick}`");
        sb.AppendLine($"- Начало сессии: {SessionStart:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine($"- Экспорт: {DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine();

        lock (LogLock)
        {
            foreach (var e in Log)
            {
                string text = (e.Text ?? "").Replace("\n", "\n> ");
                sb.AppendLine(e.Kind switch
                {
                    "message" when e.Nick == _nick => $"- **[{e.Nick}]** {text}",
                    "message"                      => $"- [{e.Nick}] {text}",
                    "system"                       => $"- *{text}*",
                    _                              => $"- **!** {text}"
                });
            }
            sb.AppendLine($"_Сообщений в сессии: {Log.Count}_");
        }

        File.WriteAllText(path, sb.ToString(), new UTF8Encoding(encoderShouldEmitUTF8Identifier: true));
        PrintAbove($"* журнал сохранён: {path}", ConsoleColor.Green);
    }

    private static async Task SendLoopAsync()
    {
        try
        {
            while (!Cts.IsCancellationRequested)
            {
                string line = Outgoing.Take(Cts.Token);

                foreach (string part in line.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries))
                {
                    byte[] bytes = Encoding.UTF8.GetBytes(part);
                    if (bytes.Length > MaxMessageBytes)
                    {
                        PrintAbove($"! сообщение длиннее {MaxMessageBytes} байт, обрезано", ConsoleColor.Red);
                        bytes = bytes[..MaxMessageBytes];
                    }
                    await RateLimitWaitAsync();
                    await SendJsonAsync(new ChatMessage("message", Encoding.UTF8.GetString(bytes)));
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
    }

    private static async Task RateLimitWaitAsync()
    {
        var now = DateTimeOffset.UtcNow;
        while (SentTimestamps.Count > 0 && (now - SentTimestamps.Peek()).TotalMilliseconds >= 1000)
            SentTimestamps.Dequeue();

        if (SentTimestamps.Count >= RateLimit)
        {
            TimeSpan wait = TimeSpan.FromSeconds(1) - (now - SentTimestamps.Peek());
            if (wait > TimeSpan.Zero) await Task.Delay(wait, Cts.Token);
            now = DateTimeOffset.UtcNow;
            while (SentTimestamps.Count > 0 && (now - SentTimestamps.Peek()).TotalMilliseconds >= 1000)
                SentTimestamps.Dequeue();
        }
        SentTimestamps.Enqueue(DateTimeOffset.UtcNow);
    }

    private static async Task SendJsonAsync(object payload)
    {
        byte[] bytes = payload switch
        {
            JoinMessage m => JsonSerializer.SerializeToUtf8Bytes(m, AppJsonContext.Default.JoinMessage),
            ChatMessage m => JsonSerializer.SerializeToUtf8Bytes(m, AppJsonContext.Default.ChatMessage),
            _ => throw new ArgumentException("неизвестный тип сообщения")
        };
        await _ws.SendAsync(bytes, WebSocketMessageType.Text, true, Cts.Token);
    }

    private static async Task<ServerMessage?> ReceiveJsonAsync()
    {
        var buffer = new byte[4096];
        using var ms = new MemoryStream();
        while (true)
        {
            var result = await _ws.ReceiveAsync(buffer, Cts.Token);
            if (result.MessageType == WebSocketMessageType.Close) return null;
            ms.Write(buffer, 0, result.Count);
            if (result.EndOfMessage) break;
        }
        return JsonSerializer.Deserialize(ms.ToArray(), AppJsonContext.Default.ServerMessage);
    }

    private static async Task CloseQuietlyAsync()
    {
        try
        {
            if (_ws.State == WebSocketState.Open)
                await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None);
        }
        catch { }
    }

    private static bool IsValidNick(string nick, out string? error)
    {
        error = null;
        if (string.IsNullOrEmpty(nick)) { error = "ник не может быть пустым"; return false; }
        if (Encoding.UTF8.GetByteCount(nick) > MaxNickBytes) { error = "ник длиннее 32 байт"; return false; }
        if (nick.Any(c => !(char.IsAsciiLetterOrDigit(c) || c is '_' or '-')))
        { error = "в нике допустимы только латинские буквы, цифры, '_' и '-'"; return false; }
        return true;
    }

    private static string FormatIncoming(ServerMessage msg) => msg.Type switch
    {
        "message" => $"[{msg.Nick}] {msg.Text}",
        "system"  => $"* {msg.Text}",
        "error"   => $"! {msg.Text}",
        "welcome" => $"* welcome, {msg.Nick}",
        _         => $"? неизвестный тип: {msg.Type ?? "<не распознан>"}"
    };

    private static ConsoleColor ColorFor(ServerMessage msg) => msg.Type switch
    {
        "message" => msg.Nick == _nick ? ConsoleColor.Cyan : ConsoleColor.White,
        "system"  => ConsoleColor.Yellow,
        "error"   => ConsoleColor.Red,
        _         => ConsoleColor.Green
    };

    private static void PrintPrompt()
    {
        lock (ConsoleLock)
        {
            PrintPromptCore();
        }
    }

    private static void PrintPromptCore()
    {
        var prev = Console.ForegroundColor;
        Console.ForegroundColor = ConsoleColor.DarkYellow;
        Console.Write("> ");
        Console.ForegroundColor = prev;
        _promptVisible = true;
    }

    private static void PrintPlain(string text)
    {
        lock (ConsoleLock)
        {
            var prev = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.WriteLine(text);
            Console.ForegroundColor = prev;
        }
    }

    private static void PrintAbove(string text, ConsoleColor color)
    {
        lock (ConsoleLock)
        {
            if (_promptVisible)
            {
                Console.Write("\r" + new string(' ', Math.Max(Console.WindowWidth - 1, 1)) + "\r");
                _promptVisible = false;
            }
            var prev = Console.ForegroundColor;
            Console.ForegroundColor = color;
            Console.WriteLine(text);
            Console.ForegroundColor = prev;
            PrintPromptCore();
        }
    }

    private sealed record LogEntry(string Kind, string? Nick, string? Text);
}

internal sealed record JoinMessage(string Type, string Nick);
internal sealed record ChatMessage(string Type, string Text);

internal sealed record ServerMessage(string? Type, string? Nick, string? Text, string? Time);

[JsonSourceGenerationOptions(PropertyNameCaseInsensitive = true,
                            PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(ServerMessage))]
[JsonSerializable(typeof(JoinMessage))]
[JsonSerializable(typeof(ChatMessage))]
internal sealed partial class AppJsonContext : JsonSerializerContext { }
