package com.example.flinklab.service;

import com.example.flinklab.model.FlinkTraceEvent;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Bridge between Flink operators and SSE clients. The emitter list is static because Flink
 * deserializes its own copies of the operators inside the (embedded) TaskManager threads, so they
 * cannot hold a reference to this Spring bean; they call the static broadcast() instead.
 */
@Service
public class TelemetryService {

    private static final List<SseEmitter> EMITTERS = new CopyOnWriteArrayList<>();

    public SseEmitter registerClient() {
        SseEmitter emitter = new SseEmitter(0L);
        EMITTERS.add(emitter);
        emitter.onCompletion(() -> EMITTERS.remove(emitter));
        emitter.onTimeout(() -> EMITTERS.remove(emitter));
        emitter.onError(e -> EMITTERS.remove(emitter));
        try {
            // Flush the response headers right away so the browser marks the stream as open
            emitter.send(SseEmitter.event().comment("connected"));
        } catch (Exception e) {
            EMITTERS.remove(emitter);
        }
        return emitter;
    }

    public static void broadcast(FlinkTraceEvent event) {
        for (SseEmitter emitter : EMITTERS) {
            try {
                emitter.send(SseEmitter.event()
                        .name("flink-trace")
                        .data(event));
            } catch (Exception e) {
                // IOException for a closed socket, IllegalStateException for a completed emitter
                EMITTERS.remove(emitter);
            }
        }
    }
}
