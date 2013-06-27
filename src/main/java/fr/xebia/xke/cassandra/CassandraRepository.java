package fr.xebia.xke.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import fr.xebia.xke.cassandra.model.Track;
import fr.xebia.xke.cassandra.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.UUID;

public class CassandraRepository {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraRepository.class);

    private final Session session;

    public CassandraRepository(Session session) {
        this.session = session;
    }

    /**
     * Save an user by using a BoundStatement.
     * <p/>
     * http://www.datastax.com/doc-source/developer/java-driver/#quick_start/qsSimpleClientBoundStatements_t.html
     *
     * @param user the user to save
     */
    public void writeUserWithBoundStatement(User user) {
    }

    /**
     * Read a user with by using a QueryBuilder.
     * <p/>
     * http://www.datastax.com/doc-source/developer/java-driver/#reference/queryBuilder_r.html
     *
     * @param id the user id
     * @return a ResultSet of users
     */
    public ResultSet readUserWithQueryBuilder(UUID id) {
        return null;
    }

    /**
     * Insert a track with by using a QueryBuilder.
     * <p/>
     * http://www.datastax.com/doc-source/developer/java-driver/#reference/queryBuilder_r.html
     *
     * @param tracks track to save
     */
    public void writeTracksWithQueryBuilder(Iterable<Track> tracks) {
    }

    /**
     * Save an user click into a stream table.
     *
     * @param userId the user ID
     * @param when   the timestamp of the click
     * @param url    the url where the user has cliked
     * @param ttl    the Time To Live (TTL)
     * @see com.datastax.driver.core.querybuilder.Using
     */
    public void writeToClickStreamWithTTL(UUID userId, Date when, String url, Integer ttl) {
    }

    /**
     * Read user click stream between two dates.
     *
     * @param userId the user ID
     * @param start  start date of the time frame
     * @param end    end date of the time frame
     * @return a ResultSet of user click stream
     */
    public ResultSet readClickStreamByTimeframe(UUID userId, Date start, Date end) {
        return null;
    }

    /**
     * Execute an async query.
     * <p/>
     * http://www.datastax.com/doc-source/developer/java-driver/#asynchronous_t.html
     *
     * @param user   the user who like some tracks
     * @param tracks the list of tracks
     * @return a ResultSetFuture
     */
    public ResultSetFuture writeAndReadLikesAsynchronously(User user, Iterable<Track> tracks) {
        return null;
    }


    /**
     * Batch query.
     *
     * @param insertQueries insert queries
     * @see QueryBuilder
     */
    public void batchWriteUsers(List<String> insertQueries) {
    }
}
