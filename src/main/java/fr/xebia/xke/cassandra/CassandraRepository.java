package fr.xebia.xke.cassandra;

import java.util.Date;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import fr.xebia.xke.cassandra.model.Track;
import fr.xebia.xke.cassandra.model.User;

import static java.lang.String.format;

public class CassandraRepository {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraRepository.class);

    private final Session session;

    public CassandraRepository(Session session) {
        this.session = session;
    }

    /**
     * Save an user by using a CQL query string.
     * <p/>
     * http://www.datastax.com/documentation/cql/3.0/cql/cql_reference/insert_r.html
     */
    public void insertUserWithQueryString(User user) {
    }

    /**
     * Save an user by using a {@link com.datastax.driver.core.querybuilder.QueryBuilder}.
     */
    public void insertUserWithQueryBuilder(User user) {
    }

    /**
     * Read a user with by using a {@link com.datastax.driver.core.querybuilder.QueryBuilder}..
     */
    public ResultSet findUserWithQueryBuilder(UUID id) {
        return null;
    }

    /**
     * Insert a track with by using a {@link com.datastax.driver.core.PreparedStatement}.
     */
    public void insertTracksWithPreparedStatement(Iterable<Track> tracks) {
    }

    /**
     * Save an user click into a stream table.
     * </p>
     * {@link com.datastax.driver.core.querybuilder.Using}
     *
     * @param ttl the TTL (Time To Live) in seconds
     */
    public void insertUserClickStreamWithTTL(UUID userId, Date when, String url, int ttl) {
    }

    /**
     * Read user click stream between two dates asynchronously.
     */
    public ResultSet findUserClicksStreamWithinTimeFrame(UUID userId, Date start, Date end) {
        return null;
    }

    /**
     * Find user clicks stream by using pagination.
     * </p>
     * {@link {@link com.datastax.driver.core.Statement#setFetchSize(int)}}
     */
    public ResultSet findUserClicksStreamByPage(UUID userId, int pageSize) {
        return null;
    }

    /**
     * Execute an async write query {@link com.datastax.driver.core.Session#executeAsync(Statement)}
     */
    public ResultSetFuture insertUserTrackLikesAsync(UUID userId, UUID trackId) {
        return null;
    }

    /**
     * Use a batch query {@link com.datastax.driver.core.querybuilder.Batch}
     */
    public void insertUserTrackLikesByBatch(UUID userId, Set<Track> tracks) {
    }

    /**
     * {@link com.datastax.driver.core.Statement#enableTracing()}
     */
    public ResultSet findUserWithTracing(UUID userId) {
        return null;
    }

    /**
     * Utility method to print a query trace.
     * </p>
     * {@link com.datastax.driver.core.Statement#enableTracing()}
     */
    void printTrace(ExecutionInfo executionInfo) {
        LOG.trace("Host (queried)\t: {}", executionInfo.getQueriedHost());
        for (Host host : executionInfo.getTriedHosts()) {
            LOG.trace("Host (tried)\t: {}", host);
        }
        QueryTrace queryTrace = executionInfo.getQueryTrace();
        LOG.trace("Trace id\t\t: {}", queryTrace.getTraceId());
        LOG.trace("---------------------------------------+---------------+------------+--------------");
        LOG.trace("              DESCRIPTION                  TIMESTAMP        SRC       SRC ELAPSED  ");
        LOG.trace("---------------------------------------+---------------+------------+--------------");
        for (QueryTrace.Event event : queryTrace.getEvents()) {
            LOG.trace(format("%38s | %12s | %10s | %12s", event.getDescription(),
                    event.getTimestamp(),
                    event.getSource(), event.getSourceElapsedMicros()));
        }
        LOG.trace("---------------------------------------+---------------+------------+--------------");
    }
}
