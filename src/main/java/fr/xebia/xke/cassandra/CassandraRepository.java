package fr.xebia.xke.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static java.lang.String.format;

import java.util.Date;
import java.util.Set;
import java.util.UUID;

import fr.xebia.xke.cassandra.model.Track;
import fr.xebia.xke.cassandra.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.utils.UUIDs;

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
        LOG.debug("insert user: {}", user);
        session.execute("INSERT INTO user (id, name, email, age) VALUES (?,?,?,?)",
                user.getId(),
                user.getName(),
                user.getEmail(),
                user.getAge());
    }

    /**
     * Save an user by using a {@link com.datastax.driver.core.querybuilder.QueryBuilder}.
     */
    public void insertUserWithQueryBuilder(User user) {
        LOG.debug("insert user: {}", user);
        session.execute(insertInto("user")
                .value("id", user.getId())
                .value("name", user.getName())
                .value("email", user.getEmail())
                .value("age", user.getAge()));
    }

    /**
     * Read a user with by using a {@link com.datastax.driver.core.querybuilder.QueryBuilder}.
     * </p>
     * Use a Clause for the equality comparison {@link com.datastax.driver.core.querybuilder.QueryBuilder#eq(String, Object)}
     */
    public ResultSet findUserWithQueryBuilder(UUID id) {
        return session.execute(select().from("user").where(eq("id", id)));
    }

    /**
     * Insert a track with by using a {@link com.datastax.driver.core.PreparedStatement}.
     */
    public void insertTracksWithPreparedStatement(Iterable<Track> tracks) {
        PreparedStatement preparedStatement = session.prepare("INSERT INTO tracks (id, title, release, duration, tags) VALUES (?,?,?,?,?)");
        for (Track track : tracks) {
            LOG.debug("insert track: {}", track);
            BoundStatement boundStatement = preparedStatement.bind(
                    track.getId(),
                    track.getTitle(),
                    track.getRelease(),
                    track.getDuration(),
                    track.getTags());
            session.execute(boundStatement);
        }
    }

    /**
     * Save an user click into a stream table.
     * </p>
     * {@link com.datastax.driver.core.querybuilder.Using}
     *
     * @param ttl the TTL (Time To Live) in seconds
     */
    public void insertUserClickStreamWithTTL(UUID userId, Date when, String url, int ttl) {
        LOG.debug("insert into user_click_stream [{}, {}]", userId, when.getTime());
        session.execute(insertInto("user_click_stream")
                .value("user_id", userId)
                .value("when", when)
                .value("url", url)
                .using(ttl(ttl)));
    }

    /**
     * Read user click stream between two dates asynchronously.
     */
    public ResultSet findUserClicksStreamWithinTimeFrame(UUID userId, Date start, Date end) {
        return session.execute(select().from("user_click_stream")
                .where(eq("user_id", userId))
                .and(gt("when", start))
                .and(lt("when", end)));
    }

    /**
     * Find user clicks stream by using pagination.
     * </p>
     * {@link {@link com.datastax.driver.core.Statement#setFetchSize(int)}}
     */
    public ResultSet findUserClicksStreamByPage(UUID userId, int pageSize) {
        return session.execute(select().from("user_click_stream").where(eq("user_id", userId)).setFetchSize(pageSize));
    }

    /**
     * Execute an async write query {@link com.datastax.driver.core.Session#executeAsync(Statement)}
     */
    public ResultSetFuture insertUserTrackLikesAsync(UUID userId, UUID trackId) {
        LOG.debug("insert into track_likes [{}, {}]", userId, trackId);
        return session.executeAsync(
                insertInto("track_likes")
                        .value("user_id", userId)
                        .value("track_id", trackId)
        );
    }

    /**
     * Use a batch query {@link com.datastax.driver.core.querybuilder.Batch}
     */
    public void insertUserTrackLikesByBatch(UUID userId, Set<Track> tracks) {
        Batch batch = batch();
        for (Track track : tracks) {
            LOG.debug("insert into track_likes [{}, {}]", UUIDs.random(), track.getId());
            batch.add(insertInto("track_likes").value("user_id", userId).value("track_id", track.getId()));
        }
        session.execute(batch);
    }

    /**
     * {@link com.datastax.driver.core.Statement#enableTracing()}
     */
    public ResultSet findUserWithTracing(UUID userId) {
        return session.execute(select().from("user").where(eq("id", userId)).enableTracing());
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
