# Simulador de rede de filas

Requisito: JDK 17 ou superior. A biblioteca YAMLBeans foi utilizada e esta incluida em
`lib/yamlbeans.jar`

## Execucao

Clone o repositorio e entre na pasta:

```powershell
git clone https://github.com/htr-costa/t1-metodos.git
cd t1-metodos
```

Tambem pode baixar pelo GitHub em **Code > Download ZIP**, extrair o arquivo
e abrir o terminal na pasta do projeto.

Para compilar e executar no Windows:

```powershell
javac -encoding UTF-8 -cp "lib/yamlbeans.jar" NetworkQueueSimulator.java
java -cp "lib/yamlbeans.jar;." NetworkQueueSimulator model.yml
```

No Linux ou macOS, troque `;` por `:` no segundo comando.

## Configuracao

O arquivo `model.yml` ja contem a rede do Trabalho 1. Para testar outra rede,
edite os campos:

- `arrivals`: instante da primeira chegada externa em cada fila.
- `queues`: `servers` define os servidores; `capacity`, a capacidade total;
  `minArrival` e `maxArrival`, os intervalos de chegada externa;
  `minService` e `maxService`, os tempos de atendimento.
- `network`: rotas com `source`, `target` e `probability`. A probabilidade
  restante em cada origem representa a saida da rede.
- `seeds`: seeds do gerador; `rndnumbersPerSeed`: limite de aleatorios
  por execucao, compartilhado entre todas as filas.

Para capacidade ilimitada, omita `capacity` ou informe `-1`. As filas com
chegadas externas precisam dos intervalos de chegada e de uma entrada
em `arrivals`. os tempos estão em minutos

## Resultados

O programa mostra no terminal o tempo total, os tempos e probabilidades dos
estados e as perdas de cada fila. Além disso, os resultados estão incluidos no PDF enviado e em resultado.txt
