package org.example;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.workflow.Workflow;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface DurableStarterService {
  void exampleWorkflow();
}

class DurableStarterServiceImpl implements DurableStarterService {

  private static final Logger logger = LoggerFactory.getLogger(DurableStarterServiceImpl.class);
  public static final String STEPS_EVENT = "steps_event";

  private final DBOS dbos;

  public DurableStarterServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  private void sleepStep(String stepName) {
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Step {} interrupted", stepName, e);
    }
    logger.info("Completed Step {}!", stepName);
  }

  @Workflow
  @Override
  public void exampleWorkflow() {
    dbos.runStep(() -> sleepStep("1"), "stepOne");
    dbos.setEvent(STEPS_EVENT, 1);
    dbos.runStep(() -> sleepStep("2"), "stepTwo");
    dbos.setEvent(STEPS_EVENT, 2);
    dbos.runStep(() -> sleepStep("3"), "stepThree");
    dbos.setEvent(STEPS_EVENT, 3);
  }
}

public class App {
  private static final Logger logger = LoggerFactory.getLogger(App.class);
  private static final AtomicReference<String> indexHtmlContent = new AtomicReference<>(null);

  public String getGreeting() {
    return "Hello World!";
  }

  public static void main(String[] args) {

    var dbUrl = System.getenv("DBOS_SYSTEM_JDBC_URL");
    if (dbUrl == null || dbUrl.isEmpty()) {
      dbUrl = "jdbc:postgresql://localhost:5432/dbos_starter_java";
    }
    var dbUser = Objects.requireNonNullElse(System.getenv("PGUSER"), "postgres");
    var dbPassword = Objects.requireNonNullElse(System.getenv("PGPASSWORD"), "dbos");

    var dbosConfig =
        DBOSConfig.defaults("dbos-starter-java")
            .withDatabaseUrl(dbUrl)
            .withDbUser(dbUser)
            .withDbPassword(dbPassword)
            .withAppVersion("0.2.0");

    var dbos = new DBOS(dbosConfig);

    var proxy =
        dbos.registerWorkflows(DurableStarterService.class, new DurableStarterServiceImpl(dbos));

    @SuppressWarnings("unused")
    var app =
        Javalin.create(
                config -> {
                  config.startup.showJavalinBanner = false;
                  config.events.serverStarting(() -> dbos.launch());
                  config.events.serverStopping(() -> dbos.shutdown());
                  config.routes.get("/", ctx -> ctx.html(indexHtml()));
                  config.routes.get(
                      "/workflow/{taskId}",
                      ctx -> {
                        var taskId = ctx.pathParam("taskId");
                        dbos.startWorkflow(
                            () -> proxy.exampleWorkflow(), new StartWorkflowOptions(taskId));
                        ctx.status(200);
                      });
                  config.routes.get(
                      "/last_step/{taskId}",
                      ctx -> {
                        var taskId = ctx.pathParam("taskId");
                        var step =
                            (Integer)
                                dbos.getEvent(
                                    taskId,
                                    DurableStarterServiceImpl.STEPS_EVENT,
                                    Duration.ofSeconds(0));
                        ctx.result(String.valueOf(step != null ? step : 0));
                      });
                  config.routes.post(
                      "/crash",
                      ctx -> {
                        logger.warn("Crash endpoint called - terminating application");
                        Runtime.getRuntime().halt(0);
                        ctx.status(200);
                      });
                })
            .start(7070);
  }

  private static String indexHtml() {
    String content = indexHtmlContent.get();
    if (content == null) {
      String newContent;
      try (var html = App.class.getResourceAsStream("/index.html")) {
        if (html != null) {
          newContent = new String(html.readAllBytes(), StandardCharsets.UTF_8);
        } else {
          logger.error("HTML file not found");
          newContent = "<html><body><h1>Error: HTML file not found</h1></body></html>";
        }
      } catch (Exception e) {
        logger.error("Error reading HTML file", e);
        newContent = "<html><body><h1>Error loading page</h1></body></html>";
      }

      // Only set if still null (other thread might have set it)
      indexHtmlContent.compareAndSet(null, newContent);
      return indexHtmlContent.get();
    }
    return content;
  }
}
