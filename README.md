# lentitud-client

lentitud-client - клиент [lentitud](https://github.com/meltude/lentitud) на C#.

## Запуск
```
dotnet run -- -server <ws://адрес:порт/ws> -n <ник>
```

либо после Release сборки

```
dotnet publish -r <платформа> -c Release

./bin/Release/net10.0/osx-x64/publish/lentitud-client(.exe для Windows) --server <ws://адрес:порт/ws> -n <ник>
```

## Команды в чате

Клиент поддерживает Vim-подобные команды:

```
:q - выход

:c - очистка сессии

:e - экспорт сообщений сессии в markdown файл

/help - справка
```

## Экспорт сессии

Запускается командой ```:e```, создаёт файл chat-<yyyy-mm-dd_hhmmss>.md с сообщениями сессии.

Шаблон:

``` # lentitud — журнал сессии

- Сервер: `ws://localhost:8080/ws`
- Ник: `nick`
- Начало сессии: 2026-11-16 19:20:11
- Экспорт: 2026-11-16 19:20:51

- **[nick]** hello
- **[nick]** text
- **[nick]** text
_Сообщений в сессии: 3_

```
