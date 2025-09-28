package com.kaiburr;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;
import java.io.*;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.*;

@SpringBootApplication
public class TaskApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(TaskApiApplication.class, args);
    }
}

// ===== Models =====
class TaskExecution {
    private Instant startTime;
    private Instant endTime;
    private String output;

    public Instant getStartTime() { return startTime; }
    public void setStartTime(Instant startTime) { this.startTime = startTime; }
    public Instant getEndTime() { return endTime; }
    public void setEndTime(Instant endTime) { this.endTime = endTime; }
    public String getOutput() { return output; }
    public void setOutput(String output) { this.output = output; }
}

@Document(collection = "tasks")
class Task {
    @Id
    private String id;
    private String name;
    private String owner;
    private String command;
    private List<TaskExecution> taskExecutions = new ArrayList<>();

    // getters/setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }
    public String getCommand() { return command; }
    public void setCommand(String command) { this.command = command; }
    public List<TaskExecution> getTaskExecutions() { return taskExecutions; }
    public void setTaskExecutions(List<TaskExecution> taskExecutions) { this.taskExecutions = taskExecutions; }
}

// ===== Repo =====
interface TaskRepository extends MongoRepository<Task, String> {
    List<Task> findByNameContainingIgnoreCase(String name);
}

// ===== Utils =====
class CommandValidator {
    private static final Pattern FORBIDDEN = Pattern.compile(
            "(\\brm\\b|\\brm -rf\\b|\\bsudo\\b|\\bshutdown\\b|\\breboot\\b|;|\\$\\(|`)",
            Pattern.CASE_INSENSITIVE);

    public static boolean isSafe(String cmd) {
        if (cmd == null) return false;
        return !FORBIDDEN.matcher(cmd).find();
    }
}

// ===== K8s Executor =====
class K8sExecutor {
    private final KubernetesClient client;
    private final String namespace;

    public K8sExecutor() {
        Config config = new ConfigBuilder().build();
        this.client = new DefaultKubernetesClient(config);
        this.namespace = client.getNamespace() == null ? "default" : client.getNamespace();
    }

    public String runCommand(String[] command, long timeoutMs) throws Exception {
        String podName = "task-run-" + System.currentTimeMillis();
        Pod pod = new PodBuilder()
                .withNewMetadata().withName(podName).endMetadata()
                .withNewSpec()
                .withRestartPolicy("Never")
                .addNewContainer()
                .withName("runner")
                .withImage("busybox:1.36")
                .withCommand(command)
                .endContainer()
                .endSpec()
                .build();
        client.pods().inNamespace(namespace).create(pod);

        client.pods().inNamespace(namespace).withName(podName)
                .waitUntilCondition(p -> {
                    if (p == null || p.getStatus() == null) return false;
                    String phase = p.getStatus().getPhase();
                    return "Succeeded".equals(phase) || "Failed".equals(phase);
                }, timeoutMs);

        String logs = client.pods().inNamespace(namespace).withName(podName).getLog();
        client.pods().inNamespace(namespace).withName(podName).delete();
        return logs;
    }
}

// ===== Controller =====
@RestController
@RequestMapping("/tasks")
class TaskController {
    private final TaskRepository repo;
    TaskController(TaskRepository repo){ this.repo = repo; }

    @GetMapping
    public List<Task> all(){ return repo.findAll(); }

    @GetMapping("/{id}")
    public ResponseEntity<Task> getById(@PathVariable String id){
        return repo.findById(id).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/search")
    public ResponseEntity<List<Task>> search(@RequestParam String name){
        List<Task> found = repo.findByNameContainingIgnoreCase(name);
        if (found.isEmpty()) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(found);
    }

    @PutMapping
    public ResponseEntity<?> upsert(@RequestBody Task task){
        if (!CommandValidator.isSafe(task.getCommand()))
            return ResponseEntity.badRequest().body("Unsafe command");
        return ResponseEntity.ok(repo.save(task));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@PathVariable String id){
        if (!repo.existsById(id)) return ResponseEntity.notFound().build();
        repo.deleteById(id);
        return ResponseEntity.noContent().build();
    }

    @PutMapping("/{id}/execute")
    public ResponseEntity<?> execute(@PathVariable String id) {
        Optional<Task> opt = repo.findById(id);
        if (opt.isEmpty()) return ResponseEntity.notFound().build();
        Task task = opt.get();
        if (!CommandValidator.isSafe(task.getCommand()))
            return ResponseEntity.badRequest().body("Unsafe command");

        TaskExecution exec = new TaskExecution();
        exec.setStartTime(Instant.now());

        try {
            String[] cmd = {"sh","-c",task.getCommand()};
            String output;
            try {
                K8sExecutor k8s = new K8sExecutor();
                output = k8s.runCommand(cmd, 60000);
            } catch (Exception ex) {
                // Fallback local execution
                Process p = Runtime.getRuntime().exec(cmd);
                p.waitFor();
                output = new String(p.getInputStream().readAllBytes());
            }
            exec.setOutput(output);
            exec.setEndTime(Instant.now());
            task.getTaskExecutions().add(exec);
            repo.save(task);
            return ResponseEntity.ok(exec);
        } catch (Exception e) {
            exec.setEndTime(Instant.now());
            exec.setOutput("Error: " + e.getMessage());
            task.getTaskExecutions().add(exec);
            repo.save(task);
            return ResponseEntity.status(500).body(exec);
        }
    }
}
