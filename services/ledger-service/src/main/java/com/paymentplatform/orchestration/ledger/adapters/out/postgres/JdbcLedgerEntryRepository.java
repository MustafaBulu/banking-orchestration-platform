package com.paymentplatform.orchestration.ledger.adapters.out.postgres;

import com.paymentplatform.orchestration.ledger.application.port.out.LedgerEntryRepository;
import com.paymentplatform.orchestration.ledger.domain.model.LedgerEntry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;

@Repository
public class JdbcLedgerEntryRepository implements LedgerEntryRepository {

    private static final String INSERT_ENTRY_SQL = """
            INSERT INTO ledger_entries (
                entry_id, payment_id, customer_id, source_event_id, line_number,
                account_code, entry_type, amount, currency, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (source_event_id, line_number) DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;

    public JdbcLedgerEntryRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean isProcessed(String eventId) {
        String sql = "SELECT COUNT(*) FROM processed_ledger_event WHERE event_id = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, eventId);
        return count != null && count > 0;
    }

    @Override
    public void saveEntries(List<LedgerEntry> entries) {
        for (LedgerEntry entry : entries) {
            jdbcTemplate.update(
                    INSERT_ENTRY_SQL,
                    entry.entryId(),
                    entry.paymentId(),
                    entry.customerId(),
                    entry.sourceEventId(),
                    entry.lineNumber(),
                    entry.accountCode(),
                    entry.entryType().name(),
                    entry.amount(),
                    entry.currency(),
                    Timestamp.from(entry.occurredAt())
            );
        }
    }

    @Override
    public void markProcessed(String eventId, String consumerName) {
        String sql = """
                INSERT INTO processed_ledger_event (event_id, consumer_name)
                VALUES (?, ?)
                ON CONFLICT (event_id) DO NOTHING
                """;
        jdbcTemplate.update(sql, eventId, consumerName);
    }
}
