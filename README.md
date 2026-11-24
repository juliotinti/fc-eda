# Wallet Core e Balances

O Wallet Core registra transferências e publica os eventos no Kafka. O Balances consome o tópico **balances** e mantém os saldos em seu próprio MySQL. A API consulta essa projeção na porta **3003**.

## Executar

Com Docker e Docker Compose v2, execute a partir da raiz do repositório:

```sh
cd 8EventDrivenArchitecture
docker compose up -d
docker compose ps
```

Na primeira subida, os containers MySQL executam os scripts de esquema e seed em `sql/`. O serviço `kafka-init` aguarda o broker, cria os tópicos e termina com código 0. Os serviços Go aguardam a inicialização de suas dependências. Os volumes mantêm os dados entre reinicializações.

## Testar uma transferência

1. Abra [api.http](api.http) em um cliente HTTP compatível e consulte Ana e Bruno. Os saldos iniciais são **1000** e **250**.
2. Envie o `POST /transactions` ao Wallet Core na porta **8080**.
3. Consulte novamente os dois saldos. Após o consumo do evento, a primeira transferência deixa Ana com **990** e Bruno com **260**.
4. Consulte a conta desconhecida para verificar o retorno `404`.

Uma consulta bem-sucedida a `GET /balances/{account_id}` retorna `200` e JSON:

```json
{"account_id":"f8df753c-3b58-43aa-8016-12aaa4f1ea3e","balance":990}
```

## Fluxo de eventos

O Wallet Core grava a transferência e os saldos em uma transação MySQL. Em seguida, publica `TransactionCreated` no tópico `transactions` e `BalanceUpdated` no tópico `balances`. O segundo evento contém o saldo final das duas contas. O Balances grava ambos em uma única transação e confirma o offset Kafka após o commit no MySQL. A partição única preserva a ordem dos eventos e o offset por conta impede que uma reentrega sobrescreva um saldo mais recente.

A consulta tem consistência eventual. Contas novas aparecem na projeção após sua primeira transferência. Uma falha do Kafka após o commit do Wallet Core pode impedir a publicação do evento, pois a gravação no MySQL e a publicação são operações independentes.

Para voltar aos saldos iniciais, apague os volumes deste Compose com `docker compose down -v` e suba o ambiente novamente. Esse comando remove os dados locais dos bancos e do Kafka.
