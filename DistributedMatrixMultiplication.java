import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import java.util.concurrent.ExecutorService;
import com.hazelcast.map.IMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class DistributedMatrixMultiplication {

    static final int N = 200;

    public static void main(String[] args) throws Exception {

        Config config = new Config();
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        IMap<String, Double> A = hz.getMap("A");
        IMap<String, Double> B = hz.getMap("B");
        IMap<String, Double> C = hz.getMap("C");

        // Inicializar matrices
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
                A.put(i + "," + j, Math.random());
                B.put(i + "," + j, Math.random());
            }
        }

        ExecutorService executor = hz.getExecutorService("exec");
        long start = System.currentTimeMillis();
        List<Future<?>> futures = new ArrayList<>();

        // Enviar una tarea por fila
        for (int i = 0; i < N; i++) {
            futures.add(executor.submit(new RowMultiplierTask(i)));
        }

        // Esperar a que terminen
        for (Future<?> f : futures) {
            f.get();
        }

        long end = System.currentTimeMillis();

        System.out.println("Tiempo total Hazelcast: "
                + (end - start) / 1000.0 + " segundos");

        System.out.println("C[8,180] = " + C.get("8,180"));

        hz.shutdown();
    }

    // ===== TAREA DISTRIBUIDA =====
    public static class RowMultiplierTask
            implements Runnable, Serializable, HazelcastInstanceAware {

        private final int row;
        private transient HazelcastInstance hz;

        public RowMultiplierTask(int row) {
            this.row = row;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }

        @Override
        public void run() {
            IMap<String, Double> A = hz.getMap("A");
            IMap<String, Double> B = hz.getMap("B");
            IMap<String, Double> C = hz.getMap("C");

            for (int j = 0; j < N; j++) {
                double sum = 0.0;
                for (int k = 0; k < N; k++) {
                    sum += A.get(row + "," + k) * B.get(k + "," + j);
                }
                C.put(row + "," + j, sum);
            }
        }
    }
}
