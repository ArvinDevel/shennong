package me.jinsui.shennong.bench;

import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

@Slf4j
public abstract class BenchBase implements Runnable {
    @Override
    public void run() {
        try {
            execute();
        } catch (Exception e) {
            log.error("Encountered exception at running bench {} ", this, e);
        }
    }

    protected abstract void execute() throws Exception;

    protected void startPrometheusServer(int port) {
        Server server = new Server(port);
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        server.setHandler(context);
        context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
        // Add metrics about CPU, JVM memory etc.
        DefaultExports.initialize();
        try {
            server.start();
            log.info("Started Prometheus stats endpoint at {}", port);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
