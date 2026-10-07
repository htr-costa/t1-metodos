import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import com.esotericsoftware.yamlbeans.YamlReader;

/**
 * simulador de rede de filas com configuracao em yaml.
 *
 * o estado de cada fila conta os clientes em espera e em atendimento.
 * a probabilidade de um estado e o tempo nele dividido pelo tempototal.
 */
public class NetworkQueueSimulator {
    private static final Locale PT = Locale.forLanguageTag("pt-BR");
    private static final class EndOfRandom extends RuntimeException {}

    private static final class RandomStream {
        private long x;
        final long a, c, m, limit;
        final List<Double> supplied;
        long consumed;
        RandomStream(long seed, long a, long c, long m, long limit,
                     List<Double> supplied) {
            if (m < 2 || a < 0 || c < 0 || limit < 1) {
                throw new IllegalArgumentException("Parametros do gerador invalidos.");
            }
            this.x = Math.floorMod(seed, m);
            this.a = a; this.c = c; this.m = m;
            this.supplied = supplied;
            this.limit = supplied == null ? limit : supplied.size();
            if (this.limit < 1) throw new IllegalArgumentException("Lista de aleatorios vazia.");
        }
        boolean hasNext() { return consumed < limit; }
        double next() {
            if (!hasNext()) throw new EndOfRandom();
            double u;
            if (supplied != null) {
                u = supplied.get((int) consumed);
            } else {
                try {
                    x = Math.floorMod(Math.addExact(Math.multiplyExact(a, x), c), m);
                } catch (ArithmeticException overflow) {
                    x = BigInteger.valueOf(a).multiply(BigInteger.valueOf(x))
                            .add(BigInteger.valueOf(c)).mod(BigInteger.valueOf(m)).longValueExact();
                }
                u = (double) x / m;
            }
            consumed++;
            return u;
        }
        double uniform(double min, double max) { return min + (max - min) * next(); }
    }

    private static final class Route {
        final QueueNode target;
        final double probability;
        Route(QueueNode target, double probability) {
            this.target = target; this.probability = probability;
        }
    }

    private static final class QueueNode {
        final String id;
        final int servers, capacity;
        final double arrMin, arrMax, svcMin, svcMax;
        final boolean external;
        final List<Route> routes = new ArrayList<>();
        // cresce conforme aparecem novos estados nas filas sem limite
        final List<Double> stateTime = new ArrayList<>();
        int n, maxObserved;
        long lost, accepted, completions, exited;
        QueueNode(String id, Map<?, ?> p) {
            this.id = id;
            servers = integer(p, "servers", -1);
            capacity = integer(p, "capacity", -1);
            if (servers < 1 || (capacity != -1 && capacity < servers)) {
                throw new IllegalArgumentException("Servidores/capacidade invalidos em " + id);
            }
            svcMin = number(p, "minService", Double.NaN);
            svcMax = number(p, "maxService", Double.NaN);
            interval(svcMin, svcMax, "atendimento de " + id);
            external = p.containsKey("minArrival") || p.containsKey("maxArrival");
            arrMin = number(p, "minArrival", Double.NaN);
            arrMax = number(p, "maxArrival", Double.NaN);
            if (external) interval(arrMin, arrMax, "chegada de " + id);
            stateTime.add(0.0);
        }
        void ensureState(int state) {
            while (stateTime.size() <= state) stateTime.add(0.0);
        }
    }

    private static final class Event implements Comparable<Event> {
        final double time;
        final long order;
        final QueueNode source, target;
        Event(double time, long order, QueueNode source, QueueNode target) {
            this.time = time; this.order = order;
            this.source = source; this.target = target;
        }
        public int compareTo(Event other) {
            int cmp = Double.compare(time, other.time);
            return cmp != 0 ? cmp : Long.compare(order, other.order);
        }
    }

    private static final class Model {
        final Map<?, ?> root;
        final Map<?, ?> queueParameters;
        final Map<?, ?> arrivals;
        final List<?> network;
        final List<Long> seeds = new ArrayList<>();
        final List<Double> supplied;
        final long a, c, m, limit;
        Model(Path path) throws IOException {
            // remove a tag inicial
            String yaml = Files.readString(path, StandardCharsets.UTF_8)
                    .replaceFirst("^\\uFEFF", "")
                    .replaceAll("(?m)^\\s*!PARAMETERS\\s*$", "");
            YamlReader reader = new YamlReader(yaml);
            try {
                root = mapping(reader.read(), "raiz");
            } finally {
                reader.close();
            }
            queueParameters = mapping(root.get("queues"), "queues");
            if (queueParameters.isEmpty()) throw new IllegalArgumentException("Nao ha filas.");
            arrivals = root.containsKey("arrivals")
                    ? mapping(root.get("arrivals"), "arrivals") : Map.of();
            network = root.containsKey("network") ? sequence(root.get("network"), "network") : List.of();
            Map<?, ?> random = root.containsKey("random")
                    ? mapping(root.get("random"), "random") : Map.of();
            a = whole(random, "a", 1103515245L);
            c = whole(random, "c", 12345L);
            m = whole(random, "m", 2147483648L);
            limit = whole(root, "rndnumbersPerSeed", 100000L);
            if (root.containsKey("seeds")) {
                for (Object value : sequence(root.get("seeds"), "seeds")) {
                    seeds.add(Long.parseLong(value.toString()));
                }
                if (seeds.isEmpty()) throw new IllegalArgumentException("Lista de sementes vazia.");
                supplied = null;
            } else if (root.containsKey("rndnumbers")) {
                supplied = new ArrayList<>();
                for (Object value : sequence(root.get("rndnumbers"), "rndnumbers")) {
                    double u = Double.parseDouble(value.toString());
                    if (!Double.isFinite(u) || u < 0 || u >= 1) {
                        throw new IllegalArgumentException("Aleatorio deve estar em [0,1).");
                    }
                    supplied.add(u);
                }
                seeds.add(7L); // a seed nao eh usada quando os numeros vem da lista
            } else {
                supplied = null;
                seeds.add(7L);
            }
        }
    }

    private final Map<String, QueueNode> queues = new LinkedHashMap<>();
    private final PriorityQueue<Event> calendar = new PriorityQueue<>();
    private final RandomStream rnd;
    private long nextEventOrder, externalArrivals, processedEvents;
    private double time;

    private NetworkQueueSimulator(Model model, long seed) {
        rnd = new RandomStream(seed, model.a, model.c, model.m, model.limit, model.supplied);
        // ordena as filas pelo nome para manter a mesma ordem em cada execucao
        Map<String, Object> ordered = new TreeMap<>();
        model.queueParameters.forEach((id, value) -> ordered.put(id.toString(), value));
        ordered.forEach((id, value) -> queues.put(id, new QueueNode(id, mapping(value, id))));
        for (Object entry : model.network) {
            Map<?, ?> p = mapping(entry, "rota");
            QueueNode source = requireQueue(p.get("source"));
            Object destination = p.get("target");
            QueueNode target = destination == null || destination.toString().equals("-1")
                    ? null : requireQueue(destination);
            double probability = number(p, "probability", Double.NaN);
            if (!Double.isFinite(probability) || probability < 0 || probability > 1) {
                throw new IllegalArgumentException("Probabilidade invalida em " + source.id);
            }
            source.routes.add(new Route(target, probability));
        }
        for (QueueNode q : queues.values()) {
            double sum = q.routes.stream().mapToDouble(r -> r.probability).sum();
            if (sum > 1.0 + 1e-12) throw new IllegalArgumentException("Rotas somam mais de 1 em " + q.id);
            if (q.external && !model.arrivals.containsKey(q.id)) {
                throw new IllegalArgumentException("Informe a primeira chegada em arrivals para " + q.id);
            }
        }
        for (Map.Entry<?, ?> entry : model.arrivals.entrySet()) {
            QueueNode q = requireQueue(entry.getKey());
            if (!q.external) throw new IllegalArgumentException("Faltam intervalos de chegada em " + q.id);
            double first = Double.parseDouble(entry.getValue().toString());
            if (!Double.isFinite(first) || first < 0) throw new IllegalArgumentException("Primeira chegada invalida.");
        }
        for (QueueNode q : queues.values()) {
            if (q.external) addEvent(Double.parseDouble(model.arrivals.get(q.id).toString()), null, q);
        }
    }

    private QueueNode requireQueue(Object id) {
        QueueNode queue = id == null ? null : queues.get(id.toString());
        if (queue == null) throw new IllegalArgumentException("Fila desconhecida: " + id);
        return queue;
    }
    private void addEvent(double at, QueueNode source, QueueNode target) {
        if (!Double.isFinite(at) || at < time) throw new IllegalStateException("Tempo de evento invalido.");
        calendar.add(new Event(at, nextEventOrder++, source, target));
    }
    private QueueNode chooseDestination(QueueNode source) {
        if (source.routes.isEmpty()) return null;
        if (source.routes.size() == 1 && source.routes.get(0).probability == 1.0) {
            return source.routes.get(0).target; // a rota ja esta definida, entao nao precisa sortear
        }
        double u = rnd.next(), cumulative = 0.0;
        for (Route route : source.routes) {
            cumulative += route.probability;
            if (u < cumulative) return route.target;
        }
        return null;
    }
    private void startService(QueueNode q) {
        // sorteia o destino antes do tempo de atendimento
        QueueNode target = chooseDestination(q);
        double duration = rnd.uniform(q.svcMin, q.svcMax);
        addEvent(time + duration, q, target);
    }
    private void arrive(QueueNode q, boolean external) {
        if (external) externalArrivals++;
        if (q.capacity == -1 || q.n < q.capacity) {
            q.n++;
            q.accepted++;
            q.maxObserved = Math.max(q.maxObserved, q.n);
            q.ensureState(q.n);
            if (q.n <= q.servers) startService(q);
        } else {
            q.lost++;
        }
        if (external) addEvent(time + rnd.uniform(q.arrMin, q.arrMax), null, q);
    }
    private void advanceTime(double at) {
        double elapsed = at - time;
        if (elapsed < 0) throw new IllegalStateException("Calendario fora de ordem.");
        for (QueueNode q : queues.values()) {
            q.ensureState(q.n);
            q.stateTime.set(q.n, q.stateTime.get(q.n) + elapsed);
        }
        time = at;
    }
    private void run() {
        while (rnd.hasNext() && !calendar.isEmpty()) {
            Event event = calendar.remove();
            advanceTime(event.time);
            processedEvents++;
            try {
                if (event.source == null) {
                    arrive(event.target, true);
                } else {
                    QueueNode source = event.source;
                    source.n--;
                    source.completions++;
                    if (event.target == null) source.exited++;
                    // inicia o proximo atendimento na origem antes da chegada ao destino
                    if (source.n >= source.servers) startService(source);
                    if (event.target != null) arrive(event.target, false);
                }
            } catch (EndOfRandom exhausted) {
                // encerra quando acabam os aleatorios
                break;
            }
        }
        verifyStatistics();
    }
    private void verifyStatistics() {
        for (QueueNode q : queues.values()) {
            double sum = q.stateTime.stream().mapToDouble(Double::doubleValue).sum();
            if (q.n < 0 || (q.capacity != -1 && q.n > q.capacity)
                    || Math.abs(sum - time) > 1e-7 * Math.max(1.0, time)) {
                throw new IllegalStateException("Estatisticas inconsistentes em " + q.id);
            }
        }
    }
    private static String fmt(double value) { return String.format(PT, "%,.6f", value); }
    private static String probabilityFmt(double value) { return String.format(PT, "%.9f", value); }
    private void printReport(long seed, boolean supplied) {
        System.out.println("TRABALHO 1 - SIMULACAO DE REDE DE FILAS");
        System.out.println(supplied ? "Aleatorios: lista fornecida no YAML."
                : "Gerador LCG: X0=" + seed + "; a=" + rnd.a + "; c=" + rnd.c + "; M=" + rnd.m);
        System.out.println("Aleatorios consumidos na rede: " + rnd.consumed);
        System.out.println("Tempo global (minutos): " + fmt(time));
        System.out.println("Eventos processados: " + processedEvents);
        System.out.println("Encerramento: " + (rnd.hasNext() ? "calendario vazio" : "limite de aleatorios atingido"));
        for (QueueNode q : queues.values()) {
            System.out.println();
            System.out.println("Fila " + q.id + " - G/G/" + q.servers
                    + (q.capacity == -1 ? " (capacidade ilimitada)" : "/" + q.capacity));
            System.out.println("Clientes perdidos: " + q.lost);
            System.out.println("Estado n | Tempo acumulado (min) | Probabilidade | Percentual");
            int last = q.capacity == -1 ? q.maxObserved : q.capacity;
            double sum = 0;
            for (int n = 0; n <= last; n++) {
                double state = n < q.stateTime.size() ? q.stateTime.get(n) : 0.0;
                double probability = time == 0 ? 0 : state / time;
                sum += probability;
                System.out.println(n + " | " + fmt(state) + " | " + probabilityFmt(probability)
                        + " | " + fmt(100 * probability) + "%");
            }
            if (q.capacity == -1) System.out.println("Estados acima de " + last + ": nao observados nesta execucao.");
            System.out.println("Soma dos tempos: " + fmt(q.stateTime.stream().mapToDouble(Double::doubleValue).sum()));
            System.out.println("Soma das probabilidades: " + probabilityFmt(sum));
        }
    }
    private static Map<?, ?> mapping(Object value, String field) {
        if (!(value instanceof Map<?, ?>)) throw new IllegalArgumentException("Mapa YAML esperado em " + field);
        return (Map<?, ?>) value;
    }
    private static List<?> sequence(Object value, String field) {
        if (!(value instanceof List<?>)) throw new IllegalArgumentException("Lista YAML esperada em " + field);
        return (List<?>) value;
    }
    private static double number(Map<?, ?> map, String key, double fallback) {
        Object value = map.get(key);
        return value == null ? fallback : Double.parseDouble(value.toString());
    }
    private static long whole(Map<?, ?> map, String key, long fallback) {
        Object value = map.get(key);
        return value == null ? fallback : Long.parseLong(value.toString());
    }
    private static int integer(Map<?, ?> map, String key, int fallback) {
        return Math.toIntExact(whole(map, key, fallback));
    }
    private static void interval(double min, double max, String name) {
        if (!Double.isFinite(min) || !Double.isFinite(max) || min <= 0 || max < min) {
            throw new IllegalArgumentException("Intervalo invalido para " + name);
        }
    }
    public static void main(String[] args) {
        if (args.length > 1) {
            System.err.println("Uso: java -cp \"lib/yamlbeans.jar;.\" NetworkQueueSimulator [model.yml]");
            System.exit(1);
        }
        try {
            Model model = new Model(Path.of(args.length == 0 ? "model.yml" : args[0]));
            for (long seed : model.seeds) {
                NetworkQueueSimulator sim = new NetworkQueueSimulator(model, seed);
                sim.run();
                sim.printReport(seed, model.supplied != null);
            }
        } catch (IOException | IllegalArgumentException error) {
            System.err.println("Erro no modelo: " + error.getMessage());
            System.exit(1);
        }
    }
}
