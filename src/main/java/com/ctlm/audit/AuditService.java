package com.ctlm.audit;

import com.ctlm.statemachine.LcmEvent;
import com.ctlm.statemachine.LcmState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuditService {

    private final AuditLogRepository auditLogRepository;

    public void log(String cnfId, LcmState from, LcmState to, LcmEvent event, String message) {
        AuditLog entry = new AuditLog(
            cnfId,
            from != null ? from.name() : null,
            to != null ? to.name() : null,
            event != null ? event.name() : null,
            message
        );
        auditLogRepository.save(entry);
        log.info("Audit: cnfId={} {} -> {} via {}", cnfId, from, to, event);
    }

    public List<AuditLog> getHistory(String cnfId) {
        return auditLogRepository.findByCnfIdOrderByTimestampDesc(cnfId);
    }
}
